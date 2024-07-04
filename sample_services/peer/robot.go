package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-plus/v3/server"
	"github.com/gorilla/websocket"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	jsonpbMarshaler = &protojson.MarshalOptions{
		UseEnumNumbers:  true,
		EmitUnpopulated: false,
		Indent:          "",
		UseProtoNames:   true,
	}

	jsonpbUnmarshaler = &protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}
)

type (
	Session struct {
		Token        string `json:"token"`
		RefreshToken string `json:"refresh_token"`
	}

	Robot struct {
		id           int64
		ctx          context.Context
		ctxCancelFn  context.CancelFunc
		logger       *zap.Logger
		httpClient   *http.Client
		httpKey      string
		url          string
		session      *atomic.Pointer[Session]
		conn         *atomic.Pointer[websocket.Conn]
		pingTimer    *time.Timer
		pingTimerCAS *atomic.Uint32
		outgoingCh   chan *rtapi.Envelope
		router       *server.MapOf[string, chan *rtapi.Envelope]
	}
)

func NewRobot(ctx context.Context, logger *zap.Logger, url, httpKey string, id int64) *Robot {
	ctx, ctxCancelFn := context.WithCancel(ctx)
	r := &Robot{
		ctx:          ctx,
		ctxCancelFn:  ctxCancelFn,
		logger:       logger.With(zap.Int64("uid", id)),
		httpClient:   &http.Client{Timeout: time.Second * 30},
		httpKey:      httpKey,
		url:          url,
		session:      &atomic.Pointer[Session]{},
		conn:         &atomic.Pointer[websocket.Conn]{},
		pingTimer:    time.NewTimer(10 * time.Second),
		pingTimerCAS: atomic.NewUint32(1),
		outgoingCh:   make(chan *rtapi.Envelope, 16),
		id:           id,
		router:       &server.MapOf[string, chan *rtapi.Envelope]{},
	}
	return r
}

func (r *Robot) Login() error {
	url := "http://" + r.url + "/v2/account/authenticate/custom?create=true&username=&"
	formData := fmt.Sprintf(`{
		"id": "ROB-%d",
		"vars":{}
	}`, r.id)
	request, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(formData)))
	if err != nil {
		return err
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", "Basic "+r.basicAuth())
	resp, err := r.httpClient.Do(request)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	r.logger.Info("Login", zap.String("body", string(body)), zap.String("url", url), zap.Int("status", resp.StatusCode))
	var session Session
	if err := json.Unmarshal(body, &session); err != nil {
		return err
	}

	if session.Token == "" || session.RefreshToken == "" {
		return errors.New("登录失败了")
	}

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+r.url+"/ws?lang=en&status=true&token="+session.Token, nil)
	if err != nil {
		return err
	}

	r.session.Store(&session)
	r.conn.Store(conn)
	go r.loop()
	return nil
}

func (r *Robot) CreateParty() (*rtapi.Party, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_PartyCreate{
			PartyCreate: &rtapi.PartyCreate{
				Open:    true,
				MaxSize: 256,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.Party")
	if err != nil {
		return nil, err
	}
	return m.GetParty(), nil
}

func (r *Robot) PartyJoin(partyId string) (*rtapi.Party, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_PartyJoin{
			PartyJoin: &rtapi.PartyJoin{
				PartyId: partyId,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.Party")
	if err != nil {
		return nil, err
	}
	return m.GetParty(), nil
}

func (r *Robot) ChannelJoin(roomId string, ty int32) (*rtapi.Channel, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_ChannelJoin{
			ChannelJoin: &rtapi.ChannelJoin{
				Target:      roomId,
				Type:        ty,
				Persistence: wrapperspb.Bool(true),
				Hidden:      wrapperspb.Bool(false),
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.ChannelJoin")
	if err != nil {
		return nil, err
	}
	return m.GetChannel(), nil
}

func (r *Robot) ChannelLeave(channelId string) error {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_ChannelLeave{
			ChannelLeave: &rtapi.ChannelLeave{
				ChannelId: channelId,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return err
	}
	return nil
}

func (r *Robot) MatchmakerAdd() (*rtapi.MatchmakerTicket, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_MatchmakerAdd{
			MatchmakerAdd: &rtapi.MatchmakerAdd{
				MinCount:          2,
				MaxCount:          20,
				Query:             "*",
				StringProperties:  make(map[string]string),
				NumericProperties: make(map[string]float64),
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.MatchmakerTicket")
	if err != nil {
		return nil, err
	}
	return m.GetMatchmakerTicket(), nil
}

func (r *Robot) ChannelWriteMessage(channelId, content string) (*rtapi.ChannelMessageAck, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_ChannelMessageSend{
			ChannelMessageSend: &rtapi.ChannelMessageSend{
				ChannelId: channelId,
				Content:   content,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.ChannelMessageAck")
	if err != nil {
		return nil, err
	}
	return m.GetChannelMessageAck(), nil
}

func (r *Robot) PartyDataSend(partyId string, opCode int64, data []byte) error {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_PartyDataSend{
			PartyDataSend: &rtapi.PartyDataSend{
				PartyId: partyId,
				OpCode:  opCode,
				Data:    data,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return err
	}
	return nil
}

func (r *Robot) basicAuth() string {
	return base64.StdEncoding.EncodeToString([]byte(r.httpKey + ":"))
}

func (r *Robot) processOutgoing() {
	defer func() {
		r.logger.Info("processOutgoing 线程关闭")
	}()
	conn := r.conn.Load()
OutgoingLoop:
	for {
		select {
		case <-r.ctx.Done():
			return

		case <-r.pingTimer.C:
			if ok := r.pingNow(); !ok {
				break OutgoingLoop
			}

		case payload := <-r.outgoingCh:
			if err := conn.SetWriteDeadline(time.Now().Add(time.Second * 25)); err != nil {
				r.logger.Warn("Failed to set write deadline", zap.Error(err))
				break OutgoingLoop
			}

			data, _ := jsonpbMarshaler.Marshal(payload)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				r.logger.Warn("Could not write message", zap.Error(err))
				break OutgoingLoop
			}
		}
	}

	r.Close()
}

func (r *Robot) loop() {
	defer func() {
		r.logger.Info("loop 线程关闭")
	}()

	conn := r.conn.Load()
	if conn == nil {
		return
	}

	go r.processOutgoing()
	conn.SetPongHandler(func(appData string) error {
		if !r.pingTimerCAS.CompareAndSwap(1, 0) {
			return nil
		}
		defer r.pingTimerCAS.CompareAndSwap(0, 1)
		if !r.pingTimer.Stop() {
			select {
			case <-r.pingTimer.C:
			default:
			}
		}
		r.pingTimer.Reset(time.Second * 10)
		conn.SetReadDeadline(time.Now().Add(time.Second * 25))
		return nil
	})

	conn.SetReadDeadline(time.Now().Add(time.Second * 25))
IncomingLoop:
	for {

		_, message, err := conn.ReadMessage()
		if err != nil {
			r.logger.Error("读取消息失败", zap.Error(err))
			break IncomingLoop
		}

		fmt.Printf("recv:[%d] %s\n", r.id, message)
		var envelope rtapi.Envelope
		if err := jsonpbUnmarshaler.Unmarshal(message, &envelope); err != nil {
			r.logger.Error("解析JSONrwmt", zap.Error(err))
			break IncomingLoop
		}

		if envelope.Message == nil {
			continue
		}

		replyChanKey := ""
		switch envelope.Message.(type) {
		case *rtapi.Envelope_PartyPresenceEvent:
		case *rtapi.Envelope_Party:
			replyChanKey = "rtapi.Party"
		case *rtapi.Envelope_Channel:
			replyChanKey = "rtapi.ChannelJoin"
		case *rtapi.Envelope_ChannelMessageAck:
			replyChanKey = "rtapi.ChannelMessageAck"
		case *rtapi.Envelope_MatchmakerTicket:
			replyChanKey = "rtapi.MatchmakerTicket"
		case *rtapi.Envelope_Error:
			r.router.Range(func(key string, value chan *rtapi.Envelope) bool {
				select {
				case value <- &envelope:
				default:
					r.logger.Error("router chan full")
				}
				return true
			})
			continue
		}

		if replyChanKey != "" {
			if f, ok := r.router.Load(replyChanKey); ok {
				select {
				case f <- &envelope:
				default:
					r.logger.Error("router chan full")
				}
			}
		}
	}
	r.Close()
}

func (r *Robot) pingNow() bool {
	conn := r.conn.Load()
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		r.logger.Warn("Could not set write deadline to ping", zap.Error(err))
		return false
	}

	err := conn.WriteMessage(websocket.PingMessage, []byte{})
	if err != nil {
		r.logger.Warn("Could not send ping", zap.Error(err))
		return false
	}
	return true
}

func (r *Robot) Send(payload *rtapi.Envelope) error {
	// Attempt to queue messages and observe failures.
	select {
	case r.outgoingCh <- payload:
		return nil
	default:
		// The outgoing queue is full, likely because the remote client can't keep up.
		// Terminate the connection immediately because the only alternative that doesn't block the server is
		// to start dropping messages, which might cause unexpected behaviour.
		r.logger.Warn("Could not write message, session outgoing queue full")
		// Close in a goroutine as the method can block
		go r.Close()
		return errors.New("ErrSessionQueueFull")
	}
}

func (r *Robot) Close() {
	r.pingTimer.Stop()
	r.ctxCancelFn()
}

func (r *Robot) wait(id string) (*rtapi.Envelope, error) {
	ch := make(chan *rtapi.Envelope)
	ctx, cancel := context.WithTimeout(r.ctx, time.Second*30)
	r.router.Store(id, ch)
	defer func() {
		r.router.Delete(id)
		close(ch)
		cancel()
	}()

	select {
	case m := <-ch:
		switch m.Message.(type) {
		case *rtapi.Envelope_Error:
			err := m.GetError()
			return nil, status.Error(codes.Code(err.Code), err.Message)
		default:
		}

		return m, nil

	case <-ctx.Done():
	}
	return nil, status.Error(codes.DeadlineExceeded, "time-out")
}
