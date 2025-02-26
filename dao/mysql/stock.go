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

// dao 灞傜敤鏉ユ墽琛屾暟鎹簱鐩稿叧鐨勬搷浣�
// SetStock鍒濆鍖栧簱瀛樻垨鑰呮洿鏂板簱瀛�
func SetStock(ctx context.Context, goodsId, num int64) error {
	// 浣跨敤 GORM 鐨� WithContext 鏂规硶纭繚涓婁笅鏂囦紶閫�
	result := db.WithContext(ctx).
		Model(&model.Stock{}).          // 鎸囧畾鎿嶄綔鐨勬ā鍨�
		Where("goods_id = ?", goodsId). // 鎸囧畾鏌ヨ鏉′欢
		FirstOrCreate(&model.Stock{
			GoodsId:  goodsId,
			StockNum: num,
		})
		// 妫€鏌ユ槸鍚︽湁閿欒
	if result.Error != nil {
		return errno.ErrQueryFailed

	}
	// 濡傛灉璁板綍宸插瓨鍦紝鍒欐洿鏂板簱瀛樻暟閲�
	if result.RowsAffected == 0 {
		return db.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", goodsId).
			Update("Num", num).Error
	}

	return nil
}

// GetStockByGoodsId 鏍规嵁goodsId鏌ヨ搴撳瓨鏁版嵁
func GetStockByGoodsId(ctx context.Context, goodsId int64) (*model.Stock, error) {
	// 閫氳繃gorm鍘绘暟鎹簱涓幏鍙栨暟鎹�
	var data model.Stock
	err := db.WithContext(ctx).
		Model(&model.Stock{}).          // 鎸囧畾琛ㄥ悕
		Where("goods_id = ?", goodsId). // 鏌ヨ鏉′欢
		First(&data).                   // 灏嗘煡璇㈠嚭鏉ョ殑鏁版嵁濉厖鍒扮粨鏋勪綋鍙橀噺涓�
		Error
	fmt.Printf("data:%v\n", data)
	// 濡傛灉鏌ヨ鍑洪敊涓斾笉鏄┖鏁版嵁鐨勯敊
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, errno.ErrQueryFailed
	}
	zap.L().Info("鏌ヨ鍒扮殑搴撳瓨鏁版嵁", zap.Any("data", data))
	return &data, nil
}

// 鍏堝垽鏂簱瀛樺啀涓嬪崟
// 涓嬪崟鍚庡啀鎵ｅ噺搴撳瓨
// ReduceStock 鎵ｅ噺搴撳瓨 鍩轰簬redis鍒嗗竷寮忛攣鐗堟湰
func ReduceStock(ctx context.Context, goodsId, num, orderId int64) (*model.Stock, error) {
	var data model.Stock
	// 鍒涘缓key
	mutexname := fmt.Sprintf("xx-stock-%d", goodsId)
	// 鍒涘缓閿�
	mutex := redis.Rs.NewMutex(mutexname)
	// 鑾峰彇閿�
	if err := mutex.Lock(); err != nil {
		return nil, errno.ErrReducestockFailed
	}
	// 姝ゆ椂data鍙兘閮芥槸鏃ф暟鎹� data.num = 99  瀹為檯涓婃暟鎹簱涓璶um=97
	defer mutex.Unlock() // 閲婃斁閿�
	err := db.Transaction(func(tx *gorm.DB) error {
		// 鏌ヨ搴撳瓨
		err := tx.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", goodsId).
			First(&data).Error
		if err != nil {
			zap.L().Error("鏌ヨ搴撳瓨澶辫触", zap.Int64("goods_id", goodsId), zap.Error(err))
			return err
		}
		zap.L().Info("鏌ヨ鍒扮殑搴撳瓨鏁版嵁", zap.Any("data", data))

		// 妫€鏌ュ簱瀛樻槸鍚﹁冻澶�
		availableStock := data.StockNum - data.Lock
		if availableStock < num {
			zap.L().Error("搴撳瓨涓嶈冻", zap.Int64("goods_id", goodsId), zap.Int64("available_stock", availableStock), zap.Int64("requested_num", num))
			return errno.ErrUnderstock
		}

		// 鎵ｅ噺搴撳瓨
		data.StockNum -= num
		data.Lock += num

		// 鏇存柊搴撳瓨
		err = tx.WithContext(ctx).
			Save(&data).Error
		if err != nil {
			zap.L().Error("鏇存柊搴撳瓨澶辫触", zap.Int64("goods_id", goodsId), zap.Error(err))
			return err
		}

		// 鍒涘缓搴撳瓨璁板綍
		stockRecord := model.StockRecord{
			OrderId: orderId,
			GoodsId: goodsId,
			Num:     num,
			Status:  1, // 棰勬墸鍑�
		}
		err = tx.WithContext(ctx).
			Model(&model.StockRecord{}).
			Create(&stockRecord).Error
		if err != nil {
			zap.L().Error("鍒涘缓搴撳瓨璁板綍澶辫触", zap.Error(err))
			return err
		}
		return nil
	})

	if err != nil {
		zap.L().Error("搴撳瓨鎵ｅ噺浜嬪姟澶辫触", zap.Int64("goods_id", goodsId), zap.Error(err))
		return nil, err
	}

	zap.L().Info("搴撳瓨鎵ｅ噺鎴愬姛",
		zap.Int64("goods_id", goodsId),
		zap.Int64("num", num),
		zap.Int64("new_stock_num", data.StockNum),
	)
	return &data, nil
}

// RollbackStockByMsg 鐩戝惉 RocketMQ 娑堟伅杩涜搴撳瓨鍥炴粴
func RollbackStockByMsg(ctx context.Context, data model.StockRecord) error {
	// 鍏堟煡璇㈠簱瀛樻暟鎹紝闇€瑕佹斁鍒颁簨鍔℃搷浣滀腑
	db.Transaction(func(tx *gorm.DB) error {
		var stockRecord model.StockRecord
		// 鏌ヨ搴撳瓨鎵ｅ噺璁板綍琛�
		err := tx.WithContext(ctx).
			Model(&model.StockRecord{}).
			Where("order_id = ? and goods_id = ? and status = 1", data.OrderId, data.GoodsId).
			First(&stockRecord).Error
		// 娌℃壘鍒拌褰�
		// 鍘嬫牴灏辨病璁板綍鎴栬€呭凡缁忓洖婊氳繃 涓嶉渶瑕佸悗缁搷浣�
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			zap.L().Error("query stock_record by order_id failed", zap.Error(err), zap.Int64("order_id", data.OrderId), zap.Int64("goods_id", data.GoodsId))
			return err
		}

		// 寮€濮嬪綊杩樺簱瀛�
		var stock model.Stock
		// 鏌ヨ搴撳瓨琛�
		err = tx.WithContext(ctx).
			Model(&model.Stock{}).
			Where("goods_id = ?", data.GoodsId).
			First(&stock).Error
		if err != nil {
			zap.L().Error("query stock by goods_id failed", zap.Error(err), zap.Int64("goods_id", data.GoodsId))
			return err
		}

		// 鏇存柊搴撳瓨鏁伴噺
		stock.StockNum += data.Num // 搴撳瓨鍔犱笂
		stock.Lock -= data.Num     // 閿佸畾鐨勫簱瀛樺噺鎺�
		if stock.Lock < 0 {        // 棰勬墸搴撳瓨涓嶈兘涓鸿礋
			return errno.ErrRollbackstockFailed
		}

		// 淇濆瓨搴撳瓨鏇存柊
		err = tx.WithContext(ctx).Save(&stock).Error
		if err != nil {
			zap.L().Warn("RollbackStock stock save failed", zap.Int64("goods_id", stock.GoodsId), zap.Error(err))
			return err
		}

		// 灏嗗簱瀛樻墸鍑忚褰曠殑鐘舵€佸彉鏇翠负宸插洖婊�
		stockRecord.Status = 3
		err = tx.WithContext(ctx).Save(&stockRecord).Error
		if err != nil {
			zap.L().Warn("RollbackStock stock_record save failed", zap.Int64("goods_id", stock.GoodsId), zap.Error(err))
			return err
		}

		return nil
	})
	return nil
}
