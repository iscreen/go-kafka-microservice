package dto

import "github.com/shopspring/decimal"

type CreateOrderDto struct {
	OrderId string          `json:"orderId"`
	UserId  string          `json:"userId"`
	Price   decimal.Decimal `json:"price"`
}
