package main

import (
	"context"
	"fmt"
	"stock_service/proto" // 导入库存服务的 Protobuf 生成的 Go 包
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"                      // 导入 gRPC 包
	"google.golang.org/grpc/credentials/insecure" // 导入用于不安全连接的 gRPC 凭证包
)

var (
	conn   *grpc.ClientConn  // gRPC 客户端连接
	client proto.StockClient // gRPC 客户端对象，用于调用库存服务
)

// 初始化函数，用于建立 gRPC 连接
func init() {
	var err error
	conn, err = grpc.Dial(
		"127.0.0.1:8387", // gRPC 服务地址
		grpc.WithTransportCredentials(insecure.NewCredentials()), // 使用不安全的连接（仅用于测试环境）
	)
	if err != nil {
		panic(err) // 如果连接失败，直接 panic
	}
	client = proto.NewStockClient(conn) // 创建 gRPC 客户端对象
}

// 测试库存扣减接口的函数
func TestReduceStock(wg *sync.WaitGroup, index int, errCount *int32) {
	defer wg.Done() // 在函数返回时通知 WaitGroup 当前协程已完成
	param := &proto.ReduceStockInfo{
		GoodsId: 2001,                // 商品 ID
		Num:     1,                   // 扣减数量
		OrderId: int64(index + 9001), // 订单 ID，通过索引动态生成
	}
	start := time.Now()                                          // 记录调用开始时间
	resp, err := client.ReduceStock(context.Background(), param) // 调用 gRPC 服务的库存扣减接口
	duration := time.Since(start)                                // 计算调用耗时

	if err != nil {
		atomic.AddInt32(errCount, 1) // 如果发生错误，原子操作增加错误计数
		fmt.Printf("协程 %d: 调用库存扣减接口失败: %v, 耗时: %v\n", index, err, duration)
	} else {
		fmt.Printf("协程 %d: 调用库存扣减接口成功: %+v, 耗时: %v\n", index, resp, duration)
	}
}

// 测试查询库存接口的函数
func TestGetStockByGoodsId(wg *sync.WaitGroup, errCount *int32) {
	defer wg.Done() // 在函数返回时通知 WaitGroup 当前协程已完成
	param := &proto.GetStockReq{
		GoodsId: 2001, // 商品 ID
	}
	start := time.Now()                                       // 记录调用开始时间
	resp, err := client.GetStock(context.Background(), param) // 调用 gRPC 服务的查询库存接口
	duration := time.Since(start)                             // 计算调用耗时

	if err != nil {
		atomic.AddInt32(errCount, 1) // 如果发生错误，原子操作增加错误计数
		fmt.Printf("协程: 查询库存失败: %v, 耗时: %v\n", err, duration)
	} else {
		fmt.Printf("协程: 查询库存成功: %+v, 耗时: %v\n", resp, duration)
	}
}

// 测试回滚库存接口的函数
func TestRollBackStock(wg *sync.WaitGroup, errCount *int32) {
	defer wg.Done() // 在函数返回时通知 WaitGroup 当前协程已完成
	param := &proto.RollBackStockInfo{
		GoodsId: 2001, // 商品 ID
		RollbackNum: 10,
		OrderId: 1001,
	}
	start := time.Now()                                       // 记录调用开始时间
	resp, err := client.RollbackStock(context.Background(), param) // 调用 gRPC 服务的查询库存接口
	duration := time.Since(start)                             // 计算调用耗时

	if err != nil {
		atomic.AddInt32(errCount, 1) // 如果发生错误，原子操作增加错误计数
		fmt.Printf("协程: 查询库存失败: %v, 耗时: %v\n", err, duration)
	} else {
		fmt.Printf("协程: 查询库存成功: %+v, 耗时: %v\n", resp, duration)
	}
}

func main() {
	defer conn.Close()     // 程序退出时关闭 gRPC 连接
	var wg sync.WaitGroup  // 使用 WaitGroup 等待所有协程完成
	var errCount int32 = 0 // 初始化错误计数器

	// 并发调用测试接口
	for i := 0; i < 5; i++ { // 启动 5 组并发测试
		wg.Add(1) // 每组并发调用 2 个接口（1 个扣减库存，1 个查询库存）

		//go TestGetStockByGoodsId(&wg, &errCount) // 启动协程测试查询库存接口
		//go TestReduceStock(&wg, i, &errCount)    // 启动协程测试库存扣减接口
		go TestRollBackStock(&wg,&errCount)
	}
	wg.Wait()                                             // 等待所有协程完成
	fmt.Printf("总错误数: %d\n", atomic.LoadInt32(&errCount)) // 输出总错误数
}
