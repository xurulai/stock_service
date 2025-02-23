package main

import (
	"context"
	"fmt"
	"stock_service/proto"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	conn   *grpc.ClientConn  // gRPC 客户端连接
	client proto.StockClient // gRPC 客户端实例
)

func init() {
	var err error
	conn, err = grpc.Dial(
		"127.0.0.1:8387", // gRPC 服务的地址和端口
		grpc.WithTransportCredentials(insecure.NewCredentials()), // 使用不安全的连接（仅适用于开发环境）
	)
	if err != nil {
		panic(err) // 如果连接失败，直接抛出 panic
	}
	client = proto.NewStockClient(conn) // 创建 gRPC 客户端实例
}

// TestReduceStock 测试扣减库存服务

func TestReduceStock(wg *sync.WaitGroup, index int, errCount *int32) {
	defer wg.Done()
	param := &proto.ReduceStockInfo{
		GoodsId: 2001,
		Num:     1,
		OrderId: int64(index + 8001),
	}
	start := time.Now()
	resp, err := client.ReduceStock(context.Background(), param)
	duration := time.Since(start)

	if err != nil {
		atomic.AddInt32(errCount, 1)
		fmt.Printf("协程 %d: 请求失败，错误信息: %v, 耗时: %v\n", index, err, duration)
	} else {
		fmt.Printf("协程 %d: 请求成功，响应: %+v, 耗时: %v\n", index, resp, duration)
	}
}
func TestGetStockByGoodsId(wg *sync.WaitGroup, errCount *int32) {
	defer wg.Done()
	param := &proto.GetStockReq{
		GoodsId: 2001,
	}
	start := time.Now()
	resp, err := client.GetStock(context.Background(), param)
	duration := time.Since(start)

	if err != nil {
		atomic.AddInt32(errCount, 1)
		fmt.Printf("协程: 请求失败，错误信息: %v, 耗时: %v\n", err, duration)
	} else {
		fmt.Printf("协程: 请求成功，响应: %+v, 耗时: %v\n", resp, duration)
	}
}

func main() {
	defer conn.Close()     // 程序结束时关闭 gRPC 客户端连接
	var wg sync.WaitGroup  // 使用 WaitGroup 等待所有协程完成
	var errCount int32 = 0 // 用于统计错误数量
	// 启动扣减库存的协程
	// 交错启动查询和扣减操作的协程
	for i := 0; i < 5; i++ { // 假设启动 5 组操作
		wg.Add(2) // 每组包含一个查询和一个扣减操作

		go TestGetStockByGoodsId(&wg, &errCount) // 启动查询操作
		go TestReduceStock(&wg, i, &errCount)    // 启动扣减操作
	}
	wg.Wait() // 等待所有协程完成
	fmt.Printf("测试完成，总错误数: %d\n", atomic.LoadInt32(&errCount))
}

//这是一个简单的并发测试函数，用于模拟多个协程对全局变量 num 的操作。
//num = num - 1：每次调用减 1。由于没有加锁保护，这会导致并发问题（如竞态条件）。
