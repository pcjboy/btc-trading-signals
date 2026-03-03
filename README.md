# BTC Trading Signals Microservice

A microservices-based system for monitoring BTC price data, calculating technical indicators, and generating trading signals.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     API Gateway (Go/Gin)                    │
│                    http://localhost:8888                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
      ┌───────────────────┼───────────────────┐
      │                   │                   │
      ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Data Collector│   │   Analyzer    │   │Signal Generator│
│   (Python)    │   │   (Python)    │   │    (Python)    │
│   :8001       │   │   :8002       │   │    :8003       │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  BTC API      │   │  Redis Cache  │   │  Signal DB    │
│  (External)   │   │               │   │  (SQLite)     │
└───────────────┘   └───────────────┘   └───────────────┘
```

## Services

### 1. API Gateway (Port 8888)
- Unified entry point for all services
- Routes requests to appropriate backend services
- Returns formatted trading signals

### 2. Data Collector (Port 8001)
- Fetches BTC price data from Binance API
- Stores historical data in SQLite
- Exposes REST API for OHLCV data

### 3. Analyzer (Port 8002)
- Computes technical indicators (RSI, MACD, SMA, EMA, Bollinger Bands, ATR, VWAP)
- Provides indicator values via API

### 4. Signal Generator (Port 8003)
- Generates trading signals based on indicators
- Calculates entry, exit, TP, SL levels
- Stores signals in SQLite

## Quick Start

### Using Docker Compose

```bash
cd btc-trading-signals
docker-compose up -d
```

### Using Make

```bash
make build    # Build all services
make run      # Start all services
make stop     # Stop all services
make clean    # Clean up
```

## API Endpoints

### Health Check
```bash
GET /health
```

### Get Current BTC Signal
```bash
GET /api/v1/signals/current?symbol=BTCUSDT&interval=1h
```

### Get Historical Signals
```bash
GET /api/v1/signals/history?limit=100&offset=0
```

### Get Technical Indicators
```bash
GET /api/v1/indicators?symbol=BTCUSDT&interval=1h
```

### Get OHLCV Data
```bash
GET /api/v1/ohlcv?symbol=BTCUSDT&interval=1h&limit=100
```

## Technical Indicators

| Indicator | Parameters | Description |
|-----------|------------|-------------|
| RSI | period: 14 | Relative Strength Index |
| MACD | fast: 12, slow: 26, signal: 9 | Moving Average Convergence Divergence |
| SMA | period: 20, 50, 200 | Simple Moving Average |
| EMA | period: 9, 21 | Exponential Moving Average |
| Bollinger Bands | period: 20, std: 2 | Bollinger Bands |
| ATR | period: 14 | Average True Range |
| VWAP | - | Volume Weighted Average Price |

## Signal Generation

### Entry Signals (Long)
- RSI Oversold: RSI < 30
- MACD Crossover: MACD crosses above signal line
- Golden Cross: 50 SMA crosses above 200 SMA
- Price at Lower Bollinger Band

### Exit Signals
- RSI Overbought: RSI > 70
- MACD Death Cross: MACD crosses below signal line
- Death Cross: 50 SMA crosses below 200 SMA
- Price at Upper Bollinger Band

## Environment Variables

### API Gateway
- `PORT`: Server port (default: 8080)
- `DATA_COLLECTOR_URL`: Data Collector service URL
- `ANALYZER_URL`: Analyzer service URL
- `SIGNAL_GENERATOR_URL`: Signal Generator service URL

### Data Collector
- `PORT`: Server port (default: 8001)

### Analyzer
- `PORT`: Server port (default: 8002)
- `DATA_COLLECTOR_URL`: Data Collector service URL

### Signal Generator
- `PORT`: Server port (default: 8003)
- `ANALYZER_URL`: Analyzer service URL
- `DATA_COLLECTOR_URL`: Data Collector service URL

## Technology Stack

| Service | Language | Framework |
|---------|----------|-----------|
| API Gateway | Go | Gin |
| Data Collector | Python | FastAPI |
| Analyzer | Python | FastAPI |
| Signal Generator | Python | FastAPI |

## License

MIT
