package mysql

import (
	"context"
	"fmt"
	"stock_service/dao/redis"
	"stock_service/errno"
	"stock_service/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
	//"gorm.io/gorm/clause"
)

// dao 层用来执行数据库相关的操作
// SetStock初始化库存或者更新库存
func SetStock(ctx context.Context, goodsId, num int64) error {
	// 使用 GORM 的 WithContext 方法确保上下文传递
	result := db.WithContext(ctx).
		Model(&model.Stock{}).          // 指定操作的模型
		Where("goods_id = ?", goodsId). // 指定查询条件
		FirstOrCreate(&model.Stock{
			GoodsId:  goodsId,
			StockNum: num,
		})
		// 检查是否有错误
	if result.Error != nil {
		return errno.ErrQueryFailed

	}
	// 如果记录已存在，则更新库存数量
	if result.RowsAffected == 0 {
		return db.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", goodsId).
			Update("Num", num).Error
	}

	return nil
}

// GetStockByGoodsId 根据goodsId查询库存数据
func GetStockByGoodsId(ctx context.Context, goodsId int64) (*model.Stock, error) {
	// 通过gorm去数据库中获取数据
	var data model.Stock
	err := db.WithContext(ctx).
		Model(&model.Stock{}).          // 指定表名
		Where("goods_id = ?", goodsId). // 查询条件
		First(&data).                   // 将查询出来的数据填充到结构体变量中
		Error
	fmt.Printf("data:%v\n", data)
	// 如果查询出错且不是空数据的错
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, errno.ErrQueryFailed
	}
	zap.L().Info("查询到的库存数据", zap.Any("data", data))
	return &data, nil
}

// 先判断库存再下单
// 下单后再扣减库存
// ReduceStock 扣减库存 基于redis分布式锁版本
func ReduceStock(ctx context.Context, goodsId, num, orderId int64) (*model.Stock, error) {
	var data model.Stock
	// 创建key
	mutexname := fmt.Sprintf("xx-stock-%d", goodsId)
	// 创建锁
	mutex := redis.Rs.NewMutex(mutexname)
	// 获取锁
	if err := mutex.Lock(); err != nil {
		return nil, errno.ErrReducestockFailed
	}
	// 此时data可能都是旧数据 data.num = 99  实际上数据库中num=97
	defer mutex.Unlock() // 释放锁
	err := db.Transaction(func(tx *gorm.DB) error {
		// 查询库存
		err := tx.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", goodsId).
			First(&data).Error
		if err != nil {
			zap.L().Error("查询库存失败", zap.Int64("goods_id", goodsId), zap.Error(err))
			return err
		}
		zap.L().Info("查询到的库存数据", zap.Any("data", data))

		// 检查库存是否足够
		availableStock := data.StockNum - data.Lock
		if availableStock < num {
			zap.L().Error("库存不足", zap.Int64("goods_id", goodsId), zap.Int64("available_stock", availableStock), zap.Int64("requested_num", num))
			return errno.ErrUnderstock
		}

		// 扣减库存
		data.StockNum -= num
		data.Lock += num

		// 更新库存
		err = tx.WithContext(ctx).
			Save(&data).Error
		if err != nil {
			zap.L().Error("更新库存失败", zap.Int64("goods_id", goodsId), zap.Error(err))
			return err
		}

		// 创建库存记录
		stockRecord := model.StockRecord{
			OrderId: orderId,
			GoodsId: goodsId,
			Num:     num,
			Status:  1, // 预扣减
		}
		err = tx.WithContext(ctx).
			Model(&model.StockRecord{}).
			Create(&stockRecord).Error
		if err != nil {
			zap.L().Error("创建库存记录失败", zap.Error(err))
			return err
		}
		return nil
	})

	if err != nil {
		zap.L().Error("库存扣减事务失败", zap.Int64("goods_id", goodsId), zap.Error(err))
		return nil, err
	}

	zap.L().Info("库存扣减成功",
		zap.Int64("goods_id", goodsId),
		zap.Int64("num", num),
		zap.Int64("new_stock_num", data.StockNum),
	)
	return &data, nil
}

// RollbackStockByMsg 监听 RocketMQ 消息进行库存回滚
func RollbackStockByMsg(ctx context.Context, data model.StockRecord) error {
	// 先查询库存数据，需要放到事务操作中
	db.Transaction(func(tx *gorm.DB) error {
		var sr model.StockRecord
		// 查询库存扣减记录表
		err := tx.WithContext(ctx).
			Model(&model.StockRecord{}).
			Where("order_id = ? and goods_id = ? and status = 1", data.OrderId, data.GoodsId).
			First(&sr).Error
		// 没找到记录
		// 压根就没记录或者已经回滚过 不需要后续操作
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			zap.L().Error("query stock_record by order_id failed", zap.Error(err), zap.Int64("order_id", data.OrderId), zap.Int64("goods_id", data.GoodsId))
			return err
		}

		// 开始归还库存
		var s model.Stock
		// 查询库存表
		err = tx.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", data.GoodsId).
			First(&s).Error
		if err != nil {
			zap.L().Error("query stock by goods_id failed", zap.Error(err), zap.Int64("goods_id", data.GoodsId))
			return err
		}

		// 更新库存数量
		s.StockNum += data.Num // 库存加上
		s.Lock -= data.Num     // 锁定的库存减掉
		if s.Lock < 0 {        // 预扣库存不能为负
			return errno.ErrRollbackstockFailed
		}

		// 保存库存更新
		err = tx.WithContext(ctx).Save(&s).Error
		if err != nil {
			zap.L().Warn("RollbackStock stock save failed", zap.Int64("goods_id", s.GoodsId), zap.Error(err))
			return err
		}

		// 将库存扣减记录的状态变更为已回滚
		sr.Status = 3
		err = tx.WithContext(ctx).Save(&sr).Error
		if err != nil {
			zap.L().Warn("RollbackStock stock_record save failed", zap.Int64("goods_id", s.GoodsId), zap.Error(err))
			return err
		}

		return nil
	})
	return nil
}
