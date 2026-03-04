package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func main() {
	// Get backend service URLs from environment
	dataCollectorURL := getEnv("DATA_COLLECTOR_URL", "http://localhost:8001")
	analyzerURL := getEnv("ANALYZER_URL", "http://localhost:8002")
	signalGeneratorURL := getEnv("SIGNAL_GENERATOR_URL", "http://localhost:8003")
	alphaMinerURL := getEnv("ALPHA_MINER_URL", "http://localhost:8004")
	riskModelURL := getEnv("RISK_MODEL_URL", "http://localhost:8005")
	portfolioOptimizerURL := getEnv("PORTFOLIO_OPTIMIZER_URL", "http://localhost:8006")
	orderExecutorURL := getEnv("ORDER_EXECUTOR_URL", "http://localhost:8007")

	r := gin.Default()

	// Configure CORS
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Health check endpoint
	r.GET("/health", func(c *gin.Context) {
		// Check all backend services
		services := map[string]string{
			"data_collector":      dataCollectorURL,
			"analyzer":            analyzerURL,
			"signal_generator":    signalGeneratorURL,
			"alpha_miner":         alphaMinerURL,
			"risk_model":          riskModelURL,
			"portfolio_optimizer": portfolioOptimizerURL,
			"order_executor":      orderExecutorURL,
		}

		serviceStatus := make(map[string]string)
		allHealthy := true

		for name, url := range services {
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(url + "/health")
			if err != nil || resp.StatusCode != http.StatusOK {
				serviceStatus[name] = "unhealthy"
				allHealthy = false
			} else {
				serviceStatus[name] = "healthy"
				resp.Body.Close()
			}
		}

		status := "healthy"
		httpStatus := http.StatusOK
		if !allHealthy {
			status = "degraded"
			httpStatus = http.StatusServiceUnavailable
		}

		c.JSON(httpStatus, gin.H{
			"status":   status,
			"services": serviceStatus,
		})
	})

	// Proxy to Data Collector
	r.GET("/api/v1/ohlcv", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")
		limit := c.DefaultQuery("limit", "100")

		url := fmt.Sprintf("%s/api/v1/ohlcv?symbol=%s&interval=%s&limit=%s",
			dataCollectorURL, symbol, interval, limit)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/price/current", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")

		url := fmt.Sprintf("%s/api/v1/price/current?symbol=%s", dataCollectorURL, symbol)
		proxyRequest(c, url)
	})

	// Proxy to Analyzer
	r.GET("/api/v1/analyze/indicators", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/analyze/indicators?symbol=%s&interval=%s",
			analyzerURL, symbol, interval)

		proxyRequest(c, url)
	})

	// Proxy to Signal Generator
	r.GET("/api/v1/generate/signals", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/generate/signals?symbol=%s&interval=%s",
			signalGeneratorURL, symbol, interval)

		proxyRequest(c, url)
	})

	// Unified endpoints that aggregate data from multiple services
	r.GET("/api/v1/signals/current", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		// Fetch indicators and signals in parallel
		indicatorsChan := make(chan map[string]interface{})
		signalsChan := make(chan map[string]interface{})

		go func() {
			client := &http.Client{Timeout: 10 * time.Second}
			url := fmt.Sprintf("%s/api/v1/analyze/indicators?symbol=%s&interval=%s",
				analyzerURL, symbol, interval)
			resp, err := client.Get(url)
			if err != nil {
				indicatorsChan <- nil
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				indicatorsChan <- nil
				return
			}

			var result map[string]interface{}
			if err := json.Unmarshal(body, &result); err == nil {
				indicatorsChan <- result
			} else {
				indicatorsChan <- nil
			}
		}()

		go func() {
			client := &http.Client{Timeout: 10 * time.Second}
			url := fmt.Sprintf("%s/api/v1/generate/signals?symbol=%s&interval=%s",
				signalGeneratorURL, symbol, interval)
			resp, err := client.Get(url)
			if err != nil {
				signalsChan <- nil
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				signalsChan <- nil
				return
			}

			var result map[string]interface{}
			if err := json.Unmarshal(body, &result); err == nil {
				signalsChan <- result
			} else {
				signalsChan <- nil
			}
		}()

		indicators := <-indicatorsChan
		signals := <-signalsChan

		client := &http.Client{Timeout: 5 * time.Second}
		priceURL := fmt.Sprintf("%s/api/v1/price/current?symbol=%s", dataCollectorURL, symbol)
		priceResp, _ := client.Get(priceURL)

		price := 0.0
		if priceResp != nil {
			defer priceResp.Body.Close()
			body, _ := io.ReadAll(priceResp.Body)
			var priceData map[string]interface{}
			if err := json.Unmarshal(body, &priceData); err == nil {
				if p, ok := priceData["price"].(float64); ok {
					price = p
				}
			}
		}

		// Build unified response
		response := gin.H{
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"symbol":    symbol,
			"price":     price,
		}

		if signals != nil {
			response["signal"] = signals
		}

		if indicators != nil {
			response["indicators"] = indicators
		}

		c.JSON(http.StatusOK, response)
	})

	r.GET("/api/v1/signals/history", func(c *gin.Context) {
		limit := c.DefaultQuery("limit", "100")
		offset := c.DefaultQuery("offset", "0")

		// Proxy to signal generator for historical signals
		url := fmt.Sprintf("%s/api/v1/signals/history?limit=%s&offset=%s",
			signalGeneratorURL, limit, offset)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/indicators", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/analyze/indicators?symbol=%s&interval=%s",
			analyzerURL, symbol, interval)

		proxyRequest(c, url)
	})

	// ============ Alpha Miner Endpoints ============
	r.GET("/api/v1/alpha/factors", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/alpha/factors?symbol=%s&interval=%s",
			alphaMinerURL, symbol, interval)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/alpha/score", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/alpha/score?symbol=%s&interval=%s",
			alphaMinerURL, symbol, interval)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/alpha/factor/:factor_name", func(c *gin.Context) {
		factorName := c.Param("factor_name")
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/alpha/factor/%s?symbol=%s&interval=%s",
			alphaMinerURL, factorName, symbol, interval)

		proxyRequest(c, url)
	})

	// ============ Risk Model Endpoints ============
	r.GET("/api/v1/risk/full", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/risk/full?symbol=%s&interval=%s",
			riskModelURL, symbol, interval)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/risk/var", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")
		confidence := c.DefaultQuery("confidence", "0.95")

		url := fmt.Sprintf("%s/api/v1/risk/var?symbol=%s&interval=%s&confidence=%s",
			riskModelURL, symbol, interval, confidence)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/risk/drawdown", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/risk/drawdown?symbol=%s&interval=%s",
			riskModelURL, symbol, interval)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/risk/performance", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/risk/performance?symbol=%s&interval=%s",
			riskModelURL, symbol, interval)

		proxyRequest(c, url)
	})

	// ============ Portfolio Optimizer Endpoints ============
	r.GET("/api/v1/portfolio/optimize", func(c *gin.Context) {
		symbols := c.DefaultQuery("symbols", "BTCUSDT,ETHUSDT")
		interval := c.DefaultQuery("interval", "1h")
		strategy := c.DefaultQuery("strategy", "max_sharpe")

		url := fmt.Sprintf("%s/api/v1/portfolio/optimize?symbols=%s&interval=%s&strategy=%s",
			portfolioOptimizerURL, symbols, interval, strategy)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/portfolio/allocation", func(c *gin.Context) {
		symbols := c.DefaultQuery("symbols", "BTCUSDT,ETHUSDT")
		interval := c.DefaultQuery("interval", "1h")
		capital := c.DefaultQuery("total_capital", "10000")

		url := fmt.Sprintf("%s/api/v1/portfolio/allocation?symbols=%s&interval=%s&total_capital=%s",
			portfolioOptimizerURL, symbols, interval, capital)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/portfolio/efficient-frontier", func(c *gin.Context) {
		symbols := c.DefaultQuery("symbols", "BTCUSDT,ETHUSDT")
		interval := c.DefaultQuery("interval", "1h")

		url := fmt.Sprintf("%s/api/v1/portfolio/efficient-frontier?symbols=%s&interval=%s",
			portfolioOptimizerURL, symbols, interval)

		proxyRequest(c, url)
	})

	// ============ Order Executor Endpoints ============
	r.GET("/api/v1/order/execute", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		side := c.DefaultQuery("side", "BUY")
		orderType := c.DefaultQuery("order_type", "market")
		quantity := c.DefaultQuery("quantity", "1")

		url := fmt.Sprintf("%s/api/v1/order/execute?symbol=%s&side=%s&order_type=%s&quantity=%s",
			orderExecutorURL, symbol, side, orderType, quantity)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/order/estimate", func(c *gin.Context) {
		symbol := c.DefaultQuery("symbol", "BTCUSDT")
		orderType := c.DefaultQuery("order_type", "twap")
		quantity := c.DefaultQuery("quantity", "1")

		url := fmt.Sprintf("%s/api/v1/order/estimate?symbol=%s&order_type=%s&quantity=%s",
			orderExecutorURL, symbol, orderType, quantity)

		proxyRequest(c, url)
	})

	r.GET("/api/v1/order/strategies", func(c *gin.Context) {
		url := fmt.Sprintf("%s/api/v1/order/strategies", orderExecutorURL)
		proxyRequest(c, url)
	})

	// Start server
	port := getEnv("PORT", "8080")
	log.Printf("Starting API Gateway on port %s", port)
	log.Printf("Backend services: DataCollector=%s, Analyzer=%s, SignalGenerator=%s, AlphaMiner=%s, RiskModel=%s, PortfolioOptimizer=%s, OrderExecutor=%s",
		dataCollectorURL, analyzerURL, signalGeneratorURL, alphaMinerURL, riskModelURL, portfolioOptimizerURL, orderExecutorURL)

	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func proxyRequest(c *gin.Context, url string) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{
			"error": fmt.Sprintf("Failed to reach backend: %v", err),
		})
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to read response: %v", err),
		})
		return
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to decode response: %v", err),
		})
		return
	}

	c.JSON(resp.StatusCode, result)
}
