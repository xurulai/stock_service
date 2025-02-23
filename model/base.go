package model

import (
	"time"
)

type BaseModel struct {
	ID       uint      `gorm:"primaryKey"`
	CreateAt time.Time `gorm:"autoCreateTime"`
	UpdateAt time.Time `gorm:"autoUpdateTime"`
	CreateBy string
	UpdateBy string
	Version  int16
	isDel    int8 `gorm:"index"`
}
