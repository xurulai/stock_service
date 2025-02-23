package model

// Stock 库存model
type Stock struct {
	BaseModel // 嵌入默认七个字段
	GoodsId   int64
	StockNum  int64 `gorm:"column:stocknum"` // 确保字段名与数据库一致  //库存数量
	Lock      int64 `gorm:"column:lock"`     //预扣存数
}

// TableName 声明表名
func (Stock) TableName() string {
	return "xx_stock"
}
