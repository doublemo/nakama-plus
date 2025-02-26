package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/doublemo/nakama-plus/v3/flags"
	"github.com/doublemo/nakama-plus/v3/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	protojsonMarshaler = &protojson.MarshalOptions{
		UseEnumNumbers:  true,
		EmitUnpopulated: true,
		Indent:          "",
		UseProtoNames:   true,
	}

	protojsonUnmarshaler = &protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}
)

type sayHello struct {
	logger *zap.Logger
}

func (s *sayHello) Call(ctx context.Context, in *pb.Peer_Request) (*pb.Peer_ResponseWriter, error) {
	s.logger.Info("收到请求CALL", zap.Any("request", in))
	fmt.Println("--d----", in.Context)
	return &pb.Peer_ResponseWriter{
		Context: map[string]string{
			"test": "test",
		},
		Payload: &pb.Peer_ResponseWriter_StringContent{
			StringContent: "99998888",
		},
	}, nil
}

func (s *sayHello) NotifyMsg(conn kit.Connector, in *pb.Peer_Request) {
	s.logger.Info("收到请求NotifyMsg", zap.String("name", conn.ID()), zap.String("role", conn.Role()), zap.Any("request", in))

	if err := conn.Write(&pb.Peer_ResponseWriter{
		Context: map[string]string{"test": "test"},
	}); err != nil {
		fmt.Println("发送信息失败", err)
	}

	//conn.Close()
}

func (s *sayHello) OnServiceUpdate(serviceRegistry kit.ServiceRegistry, client kit.Client) {
	s.logger.Info("收到服务更新", zap.String("name", client.Name()), zap.String("role", client.Role()))
}

// 定义版本信息
var (
	// version 版本号
	version string = "0.1.0"

	// commitid 代码提交版本号
	commitid string = "default"

	// builddate 编译日期
	builddate string = "default"
)

func main() {
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	consoleLogger := server.NewJSONLogger(os.Stdout, zapcore.InfoLevel, server.JSONFormat)

	config := parseArgs(consoleLogger, version, commitid, builddate, os.Args)
	if err := config.Check(consoleLogger); err != nil {
		consoleLogger.Panic(err.Error())
	}

	server := kit.NewServer(consoleLogger, nil, &sayHello{logger: consoleLogger}, protojsonMarshaler, protojsonUnmarshaler, config.GetServer())
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-tk.C:
				server.ConnectorRegistry().Range(func(c kit.Connector) bool {
					state := c.State()
					if state == kit.ConnectorState_WAITRETRY {
						fmt.Println("服务等待连接恢复中", c.ID())
					}

					// c.Write(&pb.Bombus_ResponseWriter{Context: map[string]*pb.OneofValue{
					// 	"test": &pb.OneofValue{Payload: &pb.OneofValue_StringValue{StringValue: "test"}},
					// }})
					return true
				})
			}
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	<-c

	server.Stop()
	ctxCancelFn()
	consoleLogger.Info("Shutdown complete")
	os.Exit(0)
}

// ParseArgs 参数解析
func parseArgs(log *zap.Logger, v, commitid, buildAt string, args []string) *Configuration {
	if len(args) > 1 {
		switch args[1] {
		case "--version", "-v":
			fmt.Printf("%s + %s + %s\n", v, commitid, buildAt)
			os.Exit(0)

		case "migrate":
			os.Exit(0)
		}
	}

	configFilePath := NewConfiguration(log)
	configFileFlagSet := flag.NewFlagSet("hawaii", flag.ExitOnError)
	configFileFlagMaker := flags.NewFlagMakerFlagSet(&flags.FlagMakingOptions{
		UseLowerCase: true,
		Flatten:      false,
		TagName:      "yaml",
		TagUsage:     "usage",
	}, configFileFlagSet)

	if _, err := configFileFlagMaker.ParseArgs(configFilePath, args[1:]); err != nil {
		log.Fatal("Could not parse command line arguments", zap.Error(err))
	}

	mainConfig := NewConfiguration(log)
	mainConfig.Config = configFilePath.Config
	if err := mainConfig.Parse(); err != nil {
		log.Fatal("could not parse config file", zap.Error(err))
	}

	mainFlagSet := flag.NewFlagSet("say_hello", flag.ExitOnError)
	mainFlagMaker := flags.NewFlagMakerFlagSet(&flags.FlagMakingOptions{
		UseLowerCase: true,
		Flatten:      false,
		TagName:      "yaml",
		TagUsage:     "usage",
	}, mainFlagSet)

	if _, err := mainFlagMaker.ParseArgs(mainConfig, args[1:]); err != nil {
		log.Fatal("Could not parse command line arguments", zap.Error(err))
	}

	if err := mainConfig.Check(log); err != nil {
		log.Fatal("Failed to check config", zap.Error(err))
	}
	return mainConfig
}
