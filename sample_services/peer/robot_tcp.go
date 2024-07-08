package main

import (
	corebytes "bytes"
	"context"
	"crypto/rc4"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-plus/v3/internal/bytes"
	"github.com/doublemo/nakama-plus/v3/internal/dh"
	"github.com/doublemo/nakama-plus/v3/server"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	socketSYN struct {
		E1     int64
		E2     int64
		Format uint8
		Token  string
		Lang   string
		Status bool
	}

	socketHeader struct {
		Seq  uint64
		Size uint16
	}

	RobotTcp struct {
		id           int64
		ctx          context.Context
		ctxCancelFn  context.CancelFunc
		logger       *zap.Logger
		httpClient   *http.Client
		httpKey      string
		url          string
		session      *atomic.Pointer[Session]
		conn         *atomic.Pointer[net.TCPConn]
		encoder      *atomic.Pointer[rc4.Cipher]
		decoder      *atomic.Pointer[rc4.Cipher]
		pingTimer    *time.Timer
		pingTimerCAS *atomic.Uint32
		outgoingCh   chan *rtapi.Envelope
		router       *server.MapOf[string, chan *rtapi.Envelope]
		x1           *big.Int
		x2           *big.Int
		e1           *big.Int
		e2           *big.Int
		seq          *atomic.Uint64
		format       uint8
	}
)

func NewRobotTCP(ctx context.Context, logger *zap.Logger, url, httpKey string, id int64, format uint8) *RobotTcp {
	ctx, ctxCancelFn := context.WithCancel(ctx)
	r := &RobotTcp{
		ctx:          ctx,
		ctxCancelFn:  ctxCancelFn,
		logger:       logger.With(zap.Int64("uid", id)),
		httpClient:   &http.Client{Timeout: time.Second * 30},
		httpKey:      httpKey,
		url:          url,
		session:      &atomic.Pointer[Session]{},
		conn:         &atomic.Pointer[net.TCPConn]{},
		encoder:      &atomic.Pointer[rc4.Cipher]{},
		decoder:      &atomic.Pointer[rc4.Cipher]{},
		pingTimer:    time.NewTimer(10 * time.Second),
		pingTimerCAS: atomic.NewUint32(1),
		outgoingCh:   make(chan *rtapi.Envelope, 16),
		id:           id,
		router:       &server.MapOf[string, chan *rtapi.Envelope]{},
		seq:          atomic.NewUint64(0),
		format:       format,
	}

	r.x1, r.e1 = dh.DHExchange()
	r.x2, r.e2 = dh.DHExchange()
	return r
}

func (r *RobotTcp) Login() error {
	url := "http://" + r.url + "/v2/account/authenticate/custom?create=true&username=&"
	formData := fmt.Sprintf(`{
		"id": "ROB-%d",
		"vars":{}
	}`, r.id)
	request, err := http.NewRequest("POST", url, corebytes.NewBuffer([]byte(formData)))
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

	host, port, err := net.SplitHostPort(r.url)
	if err != nil {
		return err
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, portInt-3))
	if err != nil {
		return err
	}

	r.session.Store(&session)
	r.conn.Store(conn.(*net.TCPConn))
	go r.loop()

	data, err := bytes.Pack(bytes.NewBytesBuffer(nil), &socketSYN{
		Format: 1,
		E1:     r.e1.Int64(),
		E2:     r.e2.Int64(),
		Token:  session.Token,
		Status: true,
	})

	if err != nil {
		return err
	}

	buffer := bytes.NewBytesBuffer(nil)
	bytes.Pack(buffer, &socketHeader{Seq: r.seq.Inc(), Size: uint16(len(data))})
	buffer.WriteBytesV2(data)
	b := buffer.Bytes()
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second * 25)); err != nil {
		return err
	}

	if _, err := conn.Write(b); err != nil {
		return err
	}
	return nil
}

func (r *RobotTcp) CreateParty() (*rtapi.Party, error) {
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

func (r *RobotTcp) PartyJoin(partyId string) (*rtapi.Party, error) {
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

func (r *RobotTcp) ChannelJoin(roomId string, ty int32) (*rtapi.Channel, error) {
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

func (r *RobotTcp) ChannelLeave(channelId string) error {
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

func (r *RobotTcp) MatchmakerAdd() (*rtapi.MatchmakerTicket, error) {
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

func (r *RobotTcp) ChannelWriteMessage(channelId, content string) (*rtapi.ChannelMessageAck, error) {
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

func (r *RobotTcp) PartyDataSend(partyId string, opCode int64, data []byte) error {
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

func (r *RobotTcp) CreateMatch(name string) (*rtapi.Match, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_MatchCreate{
			MatchCreate: &rtapi.MatchCreate{
				Name: name,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.Match")
	if err != nil {
		return nil, err
	}
	return m.GetMatch(), nil
}

func (r *RobotTcp) MatchJoin(id string, metadata map[string]string) (*rtapi.Match, error) {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_MatchJoin{
			MatchJoin: &rtapi.MatchJoin{
				Id:       &rtapi.MatchJoin_MatchId{MatchId: id},
				Metadata: metadata,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return nil, err
	}

	m, err := r.wait("rtapi.Match")
	if err != nil {
		return nil, err
	}
	return m.GetMatch(), nil
}

func (r *RobotTcp) MatchLeave(id string) error {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_MatchLeave{
			MatchLeave: &rtapi.MatchLeave{
				MatchId: id,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return err
	}

	return nil
}

func (r *RobotTcp) MatchSendData(id string, opCode int64, data []byte, presences []*rtapi.UserPresence) error {
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_MatchDataSend{
			MatchDataSend: &rtapi.MatchDataSend{
				MatchId:   id,
				OpCode:    opCode,
				Data:      data,
				Presences: presences,
				Reliable:  true,
			},
		},
	}

	if err := r.Send(envelope); err != nil {
		return err
	}
	return nil
}

func (r *RobotTcp) basicAuth() string {
	return base64.StdEncoding.EncodeToString([]byte(r.httpKey + ":"))
}

func (r *RobotTcp) processOutgoing() {
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
			var data []byte
			if r.format == uint8(server.SessionFormatJson) {
				data, _ = jsonpbMarshaler.Marshal(payload)
			} else {
				data, _ = proto.Marshal(payload)
			}

			if encoder := r.encoder.Load(); encoder != nil {
				encoder.XORKeyStream(data, data)
			}

			buffer := bytes.NewBytesBuffer(nil)
			bytes.Pack(buffer, &socketHeader{Seq: r.seq.Inc(), Size: uint16(len(data))})
			buffer.WriteBytesV2(data)

			b := buffer.Bytes()
			if err := conn.SetWriteDeadline(time.Now().Add(time.Second * 25)); err != nil {
				r.logger.Warn("Failed to set write deadline", zap.Error(err))
				break OutgoingLoop
			}

			if _, err := conn.Write(b); err != nil {
				r.logger.Warn("Could not write message", zap.Error(err))
				break OutgoingLoop
			}
		}
	}

	r.Close()
}

func (r *RobotTcp) loop() {
	defer func() {
		r.logger.Info("loop 线程关闭")
	}()

	conn := r.conn.Load()
	if conn == nil {
		return
	}

	go r.processOutgoing()
	conn.SetReadDeadline(time.Now().Add(time.Second * 25))
	header := make([]byte, 2)
IncomingLoop:
	for {

		_, err := io.ReadFull(conn, header)
		if err != nil {
			r.logger.Error("读取消息失败", zap.Error(err))
			break IncomingLoop
		}

		size := binary.BigEndian.Uint16(header)
		payload := make([]byte, size)
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			r.logger.Error("读取信息失败", zap.Error(err))
			return
		}

		if decoder := r.decoder.Load(); decoder != nil {
			decoder.XORKeyStream(payload, payload)
		} else {
			var syn socketSYN
			if err := bytes.UnPack(bytes.NewBytesBuffer(payload), &syn); err != nil {
				r.logger.Error("解析握手包失败", zap.Error(err))
				return
			}

			key1 := dh.DHKey(r.x1, big.NewInt(syn.E1))
			key2 := dh.DHKey(r.x2, big.NewInt(syn.E2))
			en, err := rc4.NewCipher([]byte(fmt.Sprintf("%v%v", "NK", key1)))
			if err != nil {
				r.logger.Error("交换KEY失败", zap.Error(err))
				return
			}

			de, err := rc4.NewCipher([]byte(fmt.Sprintf("%v%v", "NK", key2)))
			if err != nil {
				r.logger.Error("交换KEY失败", zap.Error(err))
			}

			r.encoder.Store(en)
			r.decoder.Store(de)
			continue
		}

		fmt.Printf("recv:[%d] %s\n", r.id, payload)
		var envelope rtapi.Envelope
		if r.format == uint8(server.SessionFormatJson) {
			if err := jsonpbUnmarshaler.Unmarshal(payload, &envelope); err != nil {
				r.logger.Error("解析JSONrwmt", zap.Error(err))
				break IncomingLoop
			}
		} else {
			if err := proto.Unmarshal(payload, &envelope); err != nil {
				r.logger.Error("解析JSONrwmt", zap.Error(err))
				break IncomingLoop
			}
		}

		if envelope.Message == nil {
			continue
		}

		replyChanKey := ""
		switch envelope.Message.(type) {
		case *rtapi.Envelope_Pong:
			r.pong()
			continue

		case *rtapi.Envelope_PartyPresenceEvent:
		case *rtapi.Envelope_Party:
			replyChanKey = "rtapi.Party"
		case *rtapi.Envelope_Channel:
			replyChanKey = "rtapi.ChannelJoin"
		case *rtapi.Envelope_ChannelMessageAck:
			replyChanKey = "rtapi.ChannelMessageAck"
		case *rtapi.Envelope_MatchmakerTicket:
			replyChanKey = "rtapi.MatchmakerTicket"
		case *rtapi.Envelope_Match:
			replyChanKey = "rtapi.Match"
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

func (r *RobotTcp) pingNow() bool {
	r.Send(&rtapi.Envelope{
		Message: &rtapi.Envelope_Ping{Ping: &rtapi.Ping{}},
	})
	return true
}

func (r RobotTcp) pong() error {
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
	r.conn.Load().SetReadDeadline(time.Now().Add(time.Second * 25))
	return nil
}

func (r *RobotTcp) Send(payload *rtapi.Envelope) error {
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

func (r *RobotTcp) Close() {
	r.pingTimer.Stop()
	r.ctxCancelFn()
}

func (r *RobotTcp) wait(id string) (*rtapi.Envelope, error) {
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
