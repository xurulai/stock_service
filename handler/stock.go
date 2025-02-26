package handler

import (
	"context"
	"stock_service/biz/stock"
	"stock_service/dao/mysql"
	"stock_service/model"
	"stock_service/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPC的入口

type StockSrv struct {
	proto.UnimplementedStockServer
}

// SetStock 设置库存
func (s *StockSrv) SetStock(ctx context.Context, req *proto.GoodsStockInfo) (*proto.Response, error) {
	// 参数校验
	if req.GetGoodsId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "无效的商品 ID")
	}
	if req.GetStock() < 0 {
		return nil, status.Error(codes.InvalidArgument, "库存数量不能为负")
	}

	// 调用 stock 包中的 SetStock 函数设置库存
	_, err := stock.SetStock(ctx, req.GetGoodsId(), req.GetStock())
	if err != nil {
		// 如果设置库存失败，记录错误日志并返回错误
		zap.L().Error(
			"SetStock failed",
			zap.Int64("goods_id", req.GoodsId),
			zap.Int64("num", req.GetStock()),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "设置库存失败: %v", err)
	}

	// 返回成功响应
	return &proto.Response{}, nil
}

// GetStock 获取库存数
func (s *StockSrv) GetStock(ctx context.Context, req *proto.GetStockReq) (*proto.GoodsStockInfo, error) {
	if req.GetGoodsId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "无效的商品 ID")
	}

	data, err := stock.GetStockByGoodsId(ctx, req.GetGoodsId())
	if err != nil {
		zap.L().Error(
			"GetStock failed",
			zap.Int64("goods_id", req.GoodsId),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "获取库存失败: %v", err)
	}

	return data, nil
}

// ReduceStock 扣减库存
func (s *StockSrv) ReduceStock(ctx context.Context, req *proto.ReduceStockInfo) (*proto.Response, error) {
	if req.GetGoodsId() <= 0 || req.GetNum() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "无效的参数")
	}

	_, err := stock.ReduceStock(ctx, req.GetGoodsId(), req.GetNum(), req.GetOrderId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "扣减库存失败: %v", err)
	}

	return &proto.Response{}, nil
}

// RollbackMsghandle 监听rocketmq消息进行库存回滚的处理函数
// 需考虑重复归还的问题（幂等性）  用库存扣减记录表解决
// 添加库存扣减记录表
func (s *StockSrv) RollbackStock(ctx context.Context, req *proto.RollBackStockInfo) (*proto.Response, error) {
	// 将 proto.ReduceStockInfo 转换为 model.StockRecord
	data := model.StockRecord{
		GoodsId: req.GoodsId,
		Num: req.RollbackNum,
		OrderId: req.OrderId,		
	}

	// 调用数据库层的库存回滚方法
	err := mysql.RollbackStockByMsg(ctx, data)
	if err != nil {
		// 如果库存回滚失败，记录错误并返回失败响应
		zap.L().Error("RollbackStockByMsg failed", zap.Error(err), zap.Int64("goods_id", req.GoodsId), zap.Int64("order_id", req.OrderId))
		return &proto.Response{
			Success: false,
			Message: "库存回滚失败", // 返回错误的具体原因
		}, nil
	}

	// 如果成功处理消息，返回成功结果
	return &proto.Response{
		Success: true,
		Message: "库存回滚成功",
	}, nil
}
