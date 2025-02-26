package stock

import (
	"context"
	"fmt"
	"stock_service/dao/mysql"
	"stock_service/errno"
	"stock_service/proto"
)

// biz层业务代码
// biz -> dao

// SetStock 设置库存
func SetStock(ctx context.Context, goodsId, num int64) (*proto.Response, error) {
	// 1. 调用 mysql 包中的 SetStock 方法，设置库存数量
	err := mysql.SetStock(ctx, goodsId, num)
	if err != nil {
		// 如果设置库存失败，返回错误
		return nil, errno.ErrSetstockFailed
	}
	// 2. 设置成功，返回 nil 表示操作成功
	return nil, nil
}

// GetStockByGoodsId 根据商品 ID 查询库存信息。
func GetStockByGoodsId(ctx context.Context, goodsId int64) (*proto.GoodsStockInfo, error) {
	// 1. 先去 xx_stock 表根据 goods_id 查询出库存数
	// 调用 mysql 包中的 GetStockByGoodsId 方法，从数据库中查询库存信息。
	data, err := mysql.GetStockByGoodsId(ctx, goodsId)
	if err != nil {
		// 如果查询失败，返回错误。
		return nil, err
	}

	// 2. 处理数据
	// 将查询结果封装到 Protobuf 消息 GoodsStockInfo 中。
	resp := &proto.GoodsStockInfo{
		GoodsId: data.GoodsId,  // 设置商品 ID
		Stock:   data.StockNum, // 设置库存数量
	}

	// 返回封装好的 Protobuf 消息和 nil 错误。
	return resp, nil
}

// 分布式程序中，本机加锁只能保证这一台机器不会并发修改数据，不能保证别的机器
// 批量扣减库存要用到事务，比如a买10件，b买15件这种业务场景
func ReduceStock(ctx context.Context, goodsId, num, orderId int64) (*proto.Response, error) {
	// 数据层返回的model数据
	data, err := mysql.ReduceStock(ctx, goodsId, num, orderId)
	fmt.Printf("data:%v err:%v\n", data, err)
	if err != nil {
		return nil, errno.ErrUnderstock
	}
	return &proto.Response{}, nil
}
