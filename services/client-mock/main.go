package main

import (
	"os"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

var down atomic.Bool

func main() {
	r := gin.Default()
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": status()}) })
	r.GET("/api/status", func(c *gin.Context) { c.JSON(200, gin.H{"client": os.Getenv("CLIENT_NAME"), "down": down.Load()}) })
	r.POST("/api/toggle", func(c *gin.Context) { down.Store(!down.Load()); c.JSON(200, gin.H{"down": down.Load()}) })
	r.POST("/webhook", func(c *gin.Context) {
		if down.Load() {
			c.JSON(503, gin.H{"error": "simulated client outage"})
			return
		}
		c.JSON(200, gin.H{"received": true, "client": os.Getenv("CLIENT_NAME")})
	})
	r.Run(":" + getenv("PORT", "8082"))
}
func status() string {
	if down.Load() {
		return "DOWN"
	}
	return "UP"
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
