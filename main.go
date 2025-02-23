package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"stock_service/config"
	"stock_service/dao/mysql"
	"stock_service/dao/redis"
	"stock_service/handler"
	"stock_service/logger"
	"stock_service/proto"
	"stock_service/registry"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	var cfn string
	// 0. 从命令行获取配置文件路径，默认值为 "./conf/config.yaml"
	// 例如：stock_service -conf="./conf/config_qa.yaml"
	flag.StringVar(&cfn, "conf", "./conf/config.yaml", "指定配置文件路径")
	flag.Parse()

	// 1. 加载配置文件
	err := config.Init(cfn)
	if err != nil {
		panic(err) // 如果加载配置文件失败，直接退出程序
	}

	// 2. 初始化日志模块
	err = logger.Init(config.Conf.LogConfig, config.Conf.Mode)
	if err != nil {
		panic(err) // 如果初始化日志模块失败，直接退出程序
	}

	// 3. 初始化 MySQL 数据库连接
	err = mysql.Init(config.Conf.MySQLConfig)
	if err != nil {
		panic(err) // 如果初始化 MySQL 数据库失败，直接退出程序
	}

	// 初始化 Redis 连接
	err = redis.Init(config.Conf.RedisConfig)
	if err != nil {
		panic(err) // 如果初始化 Redis 失败，直接退出程序
	}

	// 4. 初始化 Consul 服务注册中心
	err = registry.Init(config.Conf.ConsulConfig.Addr)
	if err != nil {
		panic(err) // 如果初始化 Consul 失败，直接退出程序
	}

	// 监听端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Conf.RpcPort))
	if err != nil {
		panic(err)
	}

	// 创建 gRPC 服务
	s := grpc.NewServer()
	// 注册健康检查服务
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	// 注册股票服务到 gRPC 服务
	proto.RegisterStockServer(s, &handler.StockSrv{})

	// 启动 gRPC 服务
	go func() {
		err = s.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()

	// 注册服务到 Consul
	registry.Reg.RegisterService(config.Conf.Name, config.Conf.IP, config.Conf.RpcPort, nil)

	// 打印 gRPC 服务启动日志
	zap.L().Info(
		"rpc server start",
		zap.String("ip", config.Conf.IP),
		zap.Int("port", config.Conf.RpcPort),
	)

	// 服务退出时注销服务
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit // 等待退出信号

	// 注销服务
	serviceId := fmt.Sprintf("%s-%s-%d", config.Conf.Name, config.Conf.IP, config.Conf.RpcPort)
	registry.Reg.Deregister(serviceId)
}
