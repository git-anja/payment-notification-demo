package main

import (
	"context"
	"database/sql"
	"encoding/json"

	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type PaymentRequest struct {
	ClientID string `json:"clientId" binding:"required"`
	OrderID  string `json:"orderId" binding:"required"`
	Amount   int64  `json:"amount" binding:"required"`
	Currency string `json:"currency" binding:"required"`
}

type PaymentEvent struct {
	EventID   string `json:"eventId"`
	PaymentID string `json:"paymentId"`
	ClientID  string `json:"clientId"`
	OrderID   string `json:"orderId"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

var db *sql.DB
var writer *kafka.Writer

func main() {
	var err error
	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Fatal(err)
	}

	writer = &kafka.Writer{
		Addr:     kafka.TCP(os.Getenv("KAFKA_BROKERS")),
		Topic:    os.Getenv("KAFKA_TOPIC"),
		Balancer: &kafka.Hash{},
	}

	r := gin.Default()
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "UP"}) })
	r.POST("/api/payments", createPayment)
	r.Run(":" + getenv("PORT", "8080"))
}

func createPayment(c *gin.Context) {
	var req PaymentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if req.ClientID != "amazon" && req.ClientID != "udemy" {
		c.JSON(400, gin.H{"error": "clientId must be amazon or udemy"})
		return
	}

	paymentID := "pay_" + uuid.NewString()
	eventID := "evt_" + uuid.NewString()

	_, err := db.Exec(`INSERT INTO payments(payment_id,order_id,client_id,amount,currency,status)
		VALUES($1,$2,$3,$4,$5,'SUCCESS')`, paymentID, req.OrderID, req.ClientID, req.Amount, req.Currency)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	event := PaymentEvent{
		EventID: eventID, PaymentID: paymentID, ClientID: req.ClientID,
		OrderID: req.OrderID, Amount: req.Amount, Currency: req.Currency,
		Status: "SUCCESS", Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, _ := jsonMarshal(event)
	err = writer.WriteMessages(ctx, kafka.Message{Key: []byte(req.ClientID), Value: msg})
	if err != nil {
		c.JSON(500, gin.H{"error": "payment saved but event publish failed: " + err.Error()})
		return
	}

	c.JSON(201, gin.H{"paymentId": paymentID, "eventId": eventID, "status": "SUCCESS"})
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
