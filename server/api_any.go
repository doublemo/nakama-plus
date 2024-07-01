package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"github.com/gorilla/mux"
	grpcgw "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ApiServer) AnyHTTP(w http.ResponseWriter, r *http.Request) {
	// Check first token then HTTP key for authentication, and add user info to the context.
	queryParams := r.URL.Query()
	var isTokenAuth bool
	var userID uuid.UUID
	var username string
	var vars map[string]string
	var expiry int64
	if httpKey := queryParams.Get("http_key"); httpKey != "" {
		if httpKey != s.config.GetRuntime().HTTPKey {
			// HTTP key did not match.
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, err := w.Write(httpKeyInvalidBytes)
			if err != nil {
				s.logger.Debug("Error writing response to client", zap.Error(err))
			}
			return
		}
	} else if auth := r.Header["Authorization"]; len(auth) >= 1 {
		if httpKey, _, ok := parseBasicAuth(auth[0]); ok {
			if httpKey != s.config.GetRuntime().HTTPKey {
				// HTTP key did not match.
				w.Header().Set("content-type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, err := w.Write(httpKeyInvalidBytes)
				if err != nil {
					s.logger.Debug("Error writing response to client", zap.Error(err))
				}
				return
			}
		} else {
			var token string
			userID, username, vars, expiry, token, isTokenAuth = parseBearerAuth([]byte(s.config.GetSession().EncryptionKey), auth[0])
			if !isTokenAuth || !s.sessionCache.IsValidSession(userID, expiry, token) {
				// Auth token not valid or expired.
				w.Header().Set("content-type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, err := w.Write(authTokenInvalidBytes)
				if err != nil {
					s.logger.Debug("Error writing response to client", zap.Error(err))
				}
				return
			}
		}
	} else {
		// No authentication present.
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, err := w.Write(noAuthBytes)
		if err != nil {
			s.logger.Debug("Error writing response to client", zap.Error(err))
		}
		return
	}

	start := time.Now()
	var success bool
	var recvBytes, sentBytes int
	var err error
	var id string

	// After this point the RPC will be captured in metrics.
	defer func() {
		s.metrics.ApiRpc(id, time.Since(start), int64(recvBytes), int64(sentBytes), !success)
	}()

	// Check the RPC function ID.
	maybeID := mux.Vars(r)["cid"]
	maybeName, ok := mux.Vars(r)["name"]
	if !ok || maybeName == "" {
		// Missing RPC function ID.
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		sentBytes, err = w.Write(rpcIDMustBeSetBytes)
		if err != nil {
			s.logger.Debug("Error writing response to client", zap.Error(err))
		}
		return
	}

	in := api.Request{
		Cid:    maybeID,
		Name:   maybeName,
		Header: make(map[string]string),
		Query:  make(map[string]*api.Query),
	}

	// Check if we need to mimic existing GRPC Gateway behaviour or expect to receive/send unwrapped data.
	// Any value for this query parameter, including the parameter existing with an empty value, will
	// indicate that raw behaviour is expected.
	_, unwrap := queryParams["unwrap"]

	// Prepare input to function.
	if r.Method == "POST" {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			// Request body too large.
			if err.Error() == "http: request body too large" {
				w.Header().Set("content-type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				sentBytes, err = w.Write(requestBodyTooLargeBytes)
				if err != nil {
					s.logger.Debug("Error writing response to client", zap.Error(err))
				}
				return
			}

			// Other error reading request body.
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			sentBytes, err = w.Write(internalServerErrorBytes)
			if err != nil {
				s.logger.Debug("Error writing response to client", zap.Error(err))
			}
			return
		}

		recvBytes = len(b)
		in.Body = &api.Request_BytesValue{BytesValue: b}
	}

	queryParams.Del("http_key")

	uid := ""
	if isTokenAuth {
		uid = userID.String()
	}

	clientIP, clientPort := extractClientAddressFromRequest(s.logger, r)

	// Extract http headers
	for k, v := range r.Header {
		if k == "Grpc-Timeout" || len(v) < 1 {
			continue
		}

		in.Header[k] = v[0]
	}

	for k, v := range queryParams {
		in.Query[k] = &api.Query{Value: v}
	}

	request := &pb.Request{Context: make(map[string]string), Payload: &pb.Request_Envelope{Envelope: &rtapi.Envelope{
		Message: &rtapi.Envelope_Request{Request: &in},
	}}}
	request.Context["client_ip"] = clientIP
	request.Context["client_port"] = clientPort
	request.Context["userId"] = uid
	request.Context["username"] = username
	request.Context["expiry"] = strconv.FormatInt(expiry, 10)
	for k, v := range vars {
		request.Context["vars_"+k] = v
	}

	resp, err := s.internalRemoteCall(r.Context(), request)
	if err != nil {
		code, ok := status.FromError(err)
		if !ok {
			code = status.New(codes.Internal, err.Error())
		}

		response, _ := json.Marshal(map[string]interface{}{"error": err.Error(), "message": code.Message(), "code": code.Code()})
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(grpcgw.HTTPStatusFromCode(code.Code()))
		sentBytes, err = w.Write(response)
		if err != nil {
			s.logger.Debug("Error writing response to client", zap.Error(err))
		}
		return
	}

	result := make([]byte, 0)
	if envelope := resp.GetEnvelope(); envelope != nil {
		if w := envelope.GetResponseWriter(); w != nil {
			result, _ = s.protojsonMarshaler.Marshal(w)
		}
	}

	// Return the successful result.
	var response []byte
	if !unwrap {
		// GRPC Gateway equivalent behaviour.
		var err error
		response, err = json.Marshal(map[string]interface{}{"payload": string(result)})
		if err != nil {
			// Failed to encode the wrapped response.
			s.logger.Error("Error marshaling wrapped response to client", zap.Error(err))
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			sentBytes, err = w.Write(internalServerErrorBytes)
			if err != nil {
				s.logger.Debug("Error writing response to client", zap.Error(err))
			}
			return
		}
	} else {
		// "Unwrapped" response.
		response = result
	}
	if unwrap {
		if contentType := r.Header["Content-Type"]; len(contentType) > 0 {
			// Assume the request input content type is the same as the expected response.
			w.Header().Set("content-type", contentType[0])
		} else {
			// Don't know payload content-type.
			w.Header().Set("content-type", "application/json")
		}
	} else {
		// Fall back to default response content type application/json.
		w.Header().Set("content-type", "application/json")
	}
	w.WriteHeader(http.StatusOK)
	sentBytes, err = w.Write(response)
	if err != nil {
		s.logger.Debug("Error writing response to client", zap.Error(err))
		return
	}
	success = true
}

func (s *ApiServer) Any(ctx context.Context, in *api.Request) (*api.ResponseWriter, error) {
	in.HttpKey = ""
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be set")
	}

	request := &pb.Request{
		Context: make(map[string]string),
		Payload: &pb.Request_Envelope{Envelope: &rtapi.Envelope{Message: &rtapi.Envelope_Request{Request: in}}},
	}

	for k, v := range s.config.GetRuntime().Environment {
		request.Context[k] = v
	}

	if m := ctx.Value(ctxUserIDKey{}); m != nil {
		request.Context["userId"] = m.(uuid.UUID).String()
	}

	if m := ctx.Value(ctxUsernameKey{}); m != nil {
		request.Context["username"] = m.(string)
	}

	if v := ctx.Value(ctxVarsKey{}); v != nil {
		for k, v := range v.(map[string]string) {
			request.Context["vars_"+k] = v
		}
	}

	if e := ctx.Value(ctxExpiryKey{}); e != nil {
		request.Context["expiry"] = strconv.FormatInt(e.(int64), 10)
	}

	resp, err := s.internalRemoteCall(ctx, request)
	if err != nil {
		return nil, err
	}

	envelope := resp.GetEnvelope()
	if envelope == nil {
		return &api.ResponseWriter{}, nil
	}

	writer := envelope.GetResponseWriter()
	if writer == nil {
		return &api.ResponseWriter{}, nil
	}
	return writer, nil
}

func (s *ApiServer) internalRemoteCall(ctx context.Context, in *pb.Request) (*pb.ResponseWriter, error) {
	envelope := in.GetEnvelope()
	if envelope == nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid Argument")
	}

	req := envelope.GetRequest()
	if req == nil || req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Invalid Argument")
	}

	peer := s.runtime.GetPeer()
	if peer == nil {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	endpoint, ok := peer.GetServiceRegistry().Get(req.Name)
	if !ok {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	return endpoint.Do(ctx, in)
}
