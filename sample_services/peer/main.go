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
	client := NewRobot(ctx, consoleLogger, "localhost:7350", "kaXMH1i2m5BzRm5F5uvePclHSM7Zjc4g", 101111)
	if err := client.Login(); err != nil {
		consoleLogger.Fatal("登录失败", zap.Error(err))
	}

	client.CreateParty()
	time.Sleep(time.Second)
	client2 := NewRobot(ctx, consoleLogger, "localhost:8350", "kaXMH1i2m5BzRm5F5uvePclHSM7Zjc4g", 101112)
	if err := client2.Login(); err != nil {
		consoleLogger.Fatal("登录失败", zap.Error(err))
	}

	party := client.party.Load()
	client2.PartyJoin(party.PartyId)
	client.PartyDataSend(party.PartyId, 1, []byte(`{"mm":"test"}`))
	client2.PartyDataSend(party.PartyId, 2, []byte(`{"mm":"test222"}`))

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
					client2.PartyDataSend(party.PartyId, 3, []byte(`{"mm":"test222"}`))
				} else {
					client.PartyDataSend(party.PartyId, 13, []byte(`{"mm":"test222"}`))
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
