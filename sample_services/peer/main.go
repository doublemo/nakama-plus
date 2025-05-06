package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/doublemo/nakama-plus/v3/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	consoleLogger := server.NewJSONLogger(os.Stdout, zapcore.InfoLevel, server.JSONFormat)
	client := NewRobotTCP(ctx, consoleLogger, "192.168.0.127:8350", "kaXMH1i2m5BzRm5F5uvePclHSM7Zjc4g", 101111, 0)
	if err := client.Login(); err != nil {
		consoleLogger.Fatal("登录失败", zap.Error(err))
	}

	client.Ready()
	channel, err := client.ChannelJoin("room", 1)
	if err != nil {
		consoleLogger.Fatal("加入聊天室失败", zap.Error(err))
	}

	party, err := client.CreateParty()
	if err != nil {
		consoleLogger.Fatal("创建队伍失败", zap.Error(err))
	}

	// match, err := client.CreateMatch("rob")
	// if err != nil {
	// 	consoleLogger.Fatal("匹配创建失败", zap.Error(err))
	// }

	matchId, err := client.rpcCreateMatch()
	if err != nil {
		consoleLogger.Fatal("匹配创建失败", zap.Error(err))
	}

	time.Sleep(time.Second * 1)
	client2 := NewRobot(ctx, consoleLogger, "192.168.0.127:9350", "kaXMH1i2m5BzRm5F5uvePclHSM7Zjc4g", 101112)
	if err := client2.Login(); err != nil {
		consoleLogger.Fatal("登录失败", zap.Error(err))
	}

	client2.PartyJoin(party.PartyId)
	client2.ChannelJoin("room", 1)
	consoleLogger.Info("match.MatchId", zap.String("id", matchId))
	if _, err := client2.MatchJoin(matchId, make(map[string]string)); err != nil {
		consoleLogger.Fatal("加入比赛失败", zap.Error(err), zap.String("id", matchId))
	}

	client2.MatchmakerAdd()
	client.MatchmakerAdd()
	_ = channel
	go func() {
		t := time.NewTicker(time.Second * 10)
		defer t.Stop()

		i := 0
		for {
			select {
			case <-ctx.Done():
				return

			case <-t.C:
				i++

				if i%2 == 0 {
					client2.MatchSendData(matchId, 10007, []byte(`{"b":"33333"}`), nil)
					client2.PartyDataSend(party.PartyId, 3, []byte(`{"mm":"test222"}`))
					client2.ChannelWriteMessage(channel.Id, `{"body":"hello"}`)
				} else {
					client.PartyDataSend(party.PartyId, 13, []byte(`{"mm":"test222"}`))
					client.MatchSendData(matchId, 10001, []byte(`{"b":"444"}`), nil)
				}

			}
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	<-c
	ctxCancelFn()
	os.Exit(0)
}
