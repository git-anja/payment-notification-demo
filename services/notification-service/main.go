package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	EventID   string `json:"eventId"`
	PaymentID string `json:"paymentId"`
	ClientID  string `json:"clientId"`
	OrderID   string `json:"orderId"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

type Delivery struct {
	EventID     string     `json:"eventId"`
	PaymentID   string     `json:"paymentId"`
	ClientID    string     `json:"clientId"`
	Status      string     `json:"status"`
	Attempt     int        `json:"attempt"`
	NextRetryAt *time.Time `json:"nextRetryAt,omitempty"`
	LastError   string     `json:"lastError,omitempty"`
}

var db *sql.DB
var rdb *redis.Client
var breakerMu sync.Mutex
var breakers = map[string]*Breaker{}

type Breaker struct {
	State    string
	Failures int
	OpenedAt time.Time
}

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

	rdb = redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	for i := 0; i < 30; i++ {
		if err = rdb.Ping(context.Background()).Err(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	go consumeKafka()
	go retryLoop()

	r := gin.Default()
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "UP"}) })
	r.GET("/api/notifications", listNotifications)
	r.GET("/api/circuit/:client", circuitStatus)
	r.Run(":" + getenv("PORT", "8081"))
}

func consumeKafka() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{os.Getenv("KAFKA_BROKERS")},
		Topic:   os.Getenv("KAFKA_TOPIC"), GroupID: os.Getenv("KAFKA_GROUP"),
		MinBytes: 1, MaxBytes: 10e6,
	})
	defer reader.Close()
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("kafka:", err)
			time.Sleep(time.Second)
			continue
		}
		var e Event
		if json.Unmarshal(msg.Value, &e) != nil {
			continue
		}
		enqueue(e)
		// ReadMessage commits automatically after the message is returned.
	}
}

func enqueue(e Event) {
	var url string
	err := db.QueryRow(`SELECT webhook_url FROM clients WHERE client_id=$1 AND enabled=true`, e.ClientID).Scan(&url)
	if err != nil {
		log.Println("client lookup:", err)
		return
	}
	_, err = db.Exec(`INSERT INTO notification_delivery(event_id,payment_id,client_id,webhook_url,status)
		VALUES($1,$2,$3,$4,'PENDING') ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.PaymentID, e.ClientID, url)
	if err != nil {
		log.Println("enqueue:", err)
		return
	}
}

func retryLoop() {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		rows, err := db.Query(`SELECT event_id,payment_id,client_id,webhook_url,status,attempt_count,COALESCE(last_error,'')
			FROM notification_delivery WHERE status IN ('PENDING','RETRYING') AND (next_retry_at IS NULL OR next_retry_at <= NOW())
			ORDER BY created_at LIMIT 20`)
		if err != nil {
			continue
		}
		for rows.Next() {
			var d Delivery
			var url string
			if err = rows.Scan(&d.EventID, &d.PaymentID, &d.ClientID, &url, &d.Status, &d.Attempt, &d.LastError); err == nil {
				go deliver(d, url)
			}
		}
		rows.Close()
	}
}

func deliver(d Delivery, url string) {
	if !allow(d.ClientID) {
		next := time.Now().Add(time.Duration(openSeconds()) * time.Second)
		updateRetry(d.EventID, d.Attempt, "RETRYING", next, "circuit open")
		return
	}

	attempt := d.Attempt + 1
	_, _ = db.Exec(`UPDATE notification_delivery SET status='PROCESSING',attempt_count=$1,updated_at=NOW() WHERE event_id=$2`, attempt, d.EventID)

	body, _ := json.Marshal(map[string]any{
		"eventId": d.EventID, "paymentId": d.PaymentID, "clientId": d.ClientID,
		"status": "SUCCESS", "idempotencyKey": d.EventID,
	})
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", d.EventID)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
	}

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = db.Exec(`UPDATE notification_delivery SET status='DELIVERED',updated_at=NOW(),next_retry_at=NULL,last_error=NULL WHERE event_id=$1`, d.EventID)
		success(d.ClientID)
		return
	}

	errText := "HTTP failure"
	if err != nil {
		errText = err.Error()
	} else {
		errText = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	failure(d.ClientID)
	maxAttempts := getenvInt("MAX_ATTEMPTS", 5)
	if attempt >= maxAttempts {
		_, _ = db.Exec(`UPDATE notification_delivery SET status='FAILED',last_error=$1,updated_at=NOW() WHERE event_id=$2`, errText, d.EventID)
		return
	}
	delay := time.Duration(getenvInt("BASE_DELAY_SECONDS", 2)) * time.Second * time.Duration(1<<(attempt-1))
	next := time.Now().Add(delay)
	updateRetry(d.EventID, attempt, "RETRYING", next, errText)
}

func updateRetry(id string, attempt int, status string, next time.Time, errText string) {
	_, _ = db.Exec(`UPDATE notification_delivery SET status=$1,attempt_count=$2,next_retry_at=$3,last_error=$4,updated_at=NOW() WHERE event_id=$5`, status, attempt, next, errText, id)
}

func allow(client string) bool {
	breakerMu.Lock()
	defer breakerMu.Unlock()
	b := getBreaker(client)
	if b.State == "OPEN" {
		if time.Since(b.OpenedAt) >= time.Duration(openSeconds())*time.Second {
			b.State = "HALF_OPEN"
			return true
		}
		return false
	}
	return true
}
func failure(client string) {
	breakerMu.Lock()
	defer breakerMu.Unlock()
	b := getBreaker(client)
	b.Failures++
	if b.State == "HALF_OPEN" || b.Failures >= getenvInt("CIRCUIT_FAILURE_THRESHOLD", 3) {
		b.State = "OPEN"
		b.OpenedAt = time.Now()
	}
}
func success(client string) {
	breakerMu.Lock()
	defer breakerMu.Unlock()
	b := getBreaker(client)
	b.State = "CLOSED"
	b.Failures = 0
}
func getBreaker(client string) *Breaker {
	if b, ok := breakers[client]; ok {
		return b
	}
	b := &Breaker{State: "CLOSED"}
	breakers[client] = b
	return b
}
func circuitStatus(c *gin.Context) {
	client := c.Param("client")
	breakerMu.Lock()
	b := *getBreaker(client)
	breakerMu.Unlock()
	remaining := 0
	if b.State == "OPEN" {
		remaining = int(time.Until(b.OpenedAt.Add(time.Duration(openSeconds()) * time.Second)).Seconds())
		if remaining < 0 {
			remaining = 0
		}
	}
	c.JSON(200, gin.H{"clientId": client, "state": b.State, "failures": b.Failures, "cooldownRemainingSeconds": remaining})
}
func listNotifications(c *gin.Context) {
	rows, err := db.Query(`SELECT event_id,payment_id,client_id,status,attempt_count,next_retry_at,COALESCE(last_error,'') FROM notification_delivery ORDER BY created_at DESC LIMIT 50`)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, pay, client, status, errText string
		var attempt int
		var next sql.NullTime
		rows.Scan(&id, &pay, &client, &status, &attempt, &next, &errText)
		var n any = nil
		if next.Valid {
			n = next.Time.Format(time.RFC3339)
		}
		out = append(out, map[string]any{"eventId": id, "paymentId": pay, "clientId": client, "status": status, "attempt": attempt, "nextRetryAt": n, "lastError": errText})
	}
	c.JSON(200, out)
}
func openSeconds() int { return getenvInt("CIRCUIT_OPEN_SECONDS", 15) }
func getenvInt(k string, d int) int {
	v, _ := strconv.Atoi(os.Getenv(k))
	if v <= 0 {
		return d
	}
	return v
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
