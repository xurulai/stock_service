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

// SetStock 设置商品库存，如果库存记录不存在则创建，否则更新库存数量。
func SetStock(ctx context.Context, goodsId, num int64) error {
	// 使用 GORM 的 WithContext 方法确保操作在指定的上下文中执行。
	result := db.WithContext(ctx).
		Model(&model.Stock{}).          // 指定操作的模型为 Stock 表。
		Where("goods_id = ?", goodsId). // 根据商品 ID 查询库存记录。
		FirstOrCreate(&model.Stock{     // 如果记录不存在则创建，否则获取第一条记录。
			GoodsId:  goodsId,
			StockNum: num,
		})

	// 如果查询或创建失败，返回错误。
	if result.Error != nil {
		return errno.ErrQueryFailed
	}

	// 如果没有影响任何行（即记录已存在且未更新），则手动更新库存数量。
	if result.RowsAffected == 0 {
		return db.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", goodsId).
			Update("StockNum", num).Error // 更新库存数量。
	}

	return nil // 操作成功，返回 nil。
}

// GetStockByGoodsId 根据商品 ID 查询库存信息。
func GetStockByGoodsId(ctx context.Context, goodsId int64) (*model.Stock, error) {
	// 初始化一个 Stock 结构体用于存储查询结果。
	var data model.Stock

	// 使用 GORM 查询库存记录。
	err := db.WithContext(ctx).
		Model(&model.Stock{}).          // 指定操作的模型为 Stock 表。
		Where("goods_id = ?", goodsId). // 根据商品 ID 查询。
		First(&data).                   // 获取第一条记录。
		Error                           // 获取查询结果的错误信息。

	// 如果查询失败且错误不是记录未找到，则返回 ErrQueryFailed。
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, errno.ErrQueryFailed
	}

	// 记录查询结果的日志。
	zap.L().Info("查询到的库存信息", zap.Any("data", data))

	return &data, nil // 返回查询结果。
}

// ReduceStock 减少库存，支持事务和分布式锁，确保操作的原子性。
func ReduceStock(ctx context.Context, goodsId, num, orderId int64) (*model.Stock, error) {
	var data model.Stock

	// 构造分布式锁的 key。
	mutexname := fmt.Sprintf("xx-stock-%d", goodsId)

	// 创建 Redis 分布式锁。
	mutex := redis.Rs.NewMutex(mutexname)

	// 尝试获取锁。
	if err := mutex.Lock(); err != nil {
		return nil, errno.ErrReducestockFailed
	}
	defer mutex.Unlock() // 确保在函数结束时释放锁。

	// 使用 GORM 事务执行库存减少操作。
	err := db.Transaction(func(tx *gorm.DB) error {
		// 查询当前库存。
		err := tx.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", goodsId).
			First(&data).Error
		if err != nil {
			zap.L().Error("查询库存失败", zap.Int64("goods_id", goodsId), zap.Error(err))
			return err
		}
		zap.L().Info("查询到的库存信息", zap.Any("data", data))

		// 计算可用库存（当前库存 - 锁定库存）。
		availableStock := data.StockNum - data.Lock

		// 检查是否有足够的库存。
		if availableStock < num {
			zap.L().Error("库存不足", zap.Int64("goods_id", goodsId), zap.Int64("available_stock", availableStock), zap.Int64("requested_num", num))
			return errno.ErrUnderstock
		}

		// 减少库存并增加锁定库存。
		data.StockNum -= num
		data.Lock += num

		// 更新库存记录。
		err = tx.WithContext(ctx).
			Save(&data).Error
		if err != nil {
			zap.L().Error("更新库存失败", zap.Int64("goods_id", goodsId), zap.Error(err))
			return err
		}

		// 创建库存记录。
		stockRecord := model.StockRecord{
			OrderId: orderId,
			GoodsId: goodsId,
			Num:     num,
			Status:  1, // 状态为 1 表示已减少。
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

	// 如果事务失败，返回错误。
	if err != nil {
		zap.L().Error("减少库存失败", zap.Int64("goods_id", goodsId), zap.Error(err))
		return nil, err
	}

	// 记录减少库存成功的日志。
	zap.L().Info("减少库存成功",
		zap.Int64("goods_id", goodsId),
		zap.Int64("num", num),
		zap.Int64("new_stock_num", data.StockNum),
	)
	return &data, nil
} // RollbackStockByMsg 根据 RocketMQ 消息回滚库存，支持事务操作。
func RollbackStockByMsg(ctx context.Context, data model.StockRecord) error {
	// 构造分布式锁的 key。
	mutexName := fmt.Sprintf("xx-stock-%d", data.OrderId)

	// 创建 Redis 分布式锁。
	mutex := redis.Rs.NewMutex(mutexName)

	// 尝试获取锁。
	if err := mutex.Lock(); err != nil {
		zap.L().Error("获取分布式锁失败",
			zap.String("mutexName", mutexName),
			zap.Error(err))
		return errno.ErrRollbackstockFailed
	}
	// 确保在函数结束时释放锁。
	defer mutex.Unlock() // 确保在函数结束时释放锁。

	// 使用 GORM 事务执行库存回滚操作。
	return db.Transaction(func(tx *gorm.DB) error {
		var stockRecord model.StockRecord

		// 查询库存记录。
		err := tx.WithContext(ctx).
			Model(&model.StockRecord{}).
			Where("order_id = ? and goods_id = ? and status = 1", data.OrderId, data.GoodsId).
			First(&stockRecord).Error

		// 如果记录不存在，则直接返回，不做处理。
		if err == gorm.ErrRecordNotFound {
			zap.L().Warn("库存记录不存在，无需回滚",
				zap.Int64("orderId", data.OrderId),
				zap.Int64("goodsId", data.GoodsId))
			return nil
		}

		// 如果查询失败，记录错误并返回。
		if err != nil {
			zap.L().Error("根据订单 ID 查询库存记录失败",
				zap.Error(err),
				zap.Int64("orderId", data.OrderId),
				zap.Int64("goodsId", data.GoodsId))
			return err
		}

		// 查询库存信息。
		var stock model.Stock
		err = tx.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", data.GoodsId).
			First(&stock).Error
		if err != nil {
			zap.L().Error("查询库存失败",
				zap.Error(err),
				zap.Int64("goodsId", data.GoodsId))
			return err
		}

		// 回滚库存。
		stock.StockNum += data.Num // 增加库存数量。
		stock.Lock -= data.Num     // 减少锁定库存。
		if stock.Lock < 0 {        // 如果锁定库存小于 0，表示回滚失败。
			zap.L().Error("回滚库存失败，锁定库存不足",
				zap.Int64("goodsId", data.GoodsId),
				zap.Int64("stockNum", stock.StockNum),
				zap.Int64("lock", stock.Lock))
			return errno.ErrRollbackstockFailed
		}

		// 更新库存记录。
		err = tx.WithContext(ctx).Save(&stock).Error
		if err != nil {
			zap.L().Warn("回滚库存失败",
				zap.Int64("goodsId", stock.GoodsId),
				zap.Error(err))
			return err
		}

		// 更新库存记录状态为 3（表示已回滚）。
		stockRecord.Status = 3
		err = tx.WithContext(ctx).Save(&stockRecord).Error
		if err != nil {
			zap.L().Warn("更新库存记录状态失败",
				zap.Int64("goodsId", stock.GoodsId),
				zap.Error(err))
			return err
		}

		return nil
	})
}
