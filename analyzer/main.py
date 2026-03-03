from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import uvicorn

app = FastAPI(title="Analyzer Service")

DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")


async def fetch_ohlcv_data(symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
    """Fetch OHLCV data from Data Collector service"""
    async with httpx.AsyncClient() as client:
        try:
            url = f"{DATA_COLLECTOR_URL}/api/v1/ohlcv"
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            response = await client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if not data.get("data"):
                return pd.DataFrame()
            
            df = pd.DataFrame(data["data"])
            if not df.empty:
                df = df.sort_values("timestamp")
            return df
        except Exception as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame()


def calculate_rsi(prices: pd.Series, period: int = 14) -> float:
    """Calculate Relative Strength Index"""
    try:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi.iloc[-1], 2) if not rsi.empty else 50.0
    except:
        return 50.0


def calculate_macd(prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Calculate MACD"""
    try:
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            "macd": round(macd_line.iloc[-1], 2),
            "signal": round(signal_line.iloc[-1], 2),
            "histogram": round(histogram.iloc[-1], 2)
        }
    except:
        return {"macd": 0.0, "signal": 0.0, "histogram": 0.0}


def calculate_sma(prices: pd.Series, period: int) -> float:
    """Calculate Simple Moving Average"""
    try:
        sma = prices.rolling(window=period).mean()
        return round(sma.iloc[-1], 2) if not sma.empty else 0.0
    except:
        return 0.0


def calculate_ema(prices: pd.Series, period: int) -> float:
    """Calculate Exponential Moving Average"""
    try:
        ema = prices.ewm(span=period, adjust=False).mean()
        return round(ema.iloc[-1], 2) if not ema.empty else 0.0
    except:
        return 0.0


def calculate_bollinger_bands(prices: pd.Series, period: int = 20, std_dev: int = 2):
    """Calculate Bollinger Bands"""
    try:
        sma = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        return {
            "upper": round(upper_band.iloc[-1], 2),
            "middle": round(sma.iloc[-1], 2),
            "lower": round(lower_band.iloc[-1], 2)
        }
    except:
        return {"upper": 0.0, "middle": 0.0, "lower": 0.0}


def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    """Calculate Average True Range"""
    try:
        high_low = high - low
        high_close = np.abs(high - close.shift())
        low_close = np.abs(low - close.shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        
        return round(atr.iloc[-1], 2) if not atr.empty else 0.0
    except:
        return 0.0


def calculate_vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> float:
    """Calculate Volume Weighted Average Price"""
    try:
        typical_price = (high + low + close) / 3
        vwap = (typical_price * volume).cumsum() / volume.cumsum()
        return round(vwap.iloc[-1], 2) if not vwap.empty else 0.0
    except:
        return 0.0


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "analyzer"}


@app.get("/api/v1/analyze/indicators")
async def get_indicators(
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval")
):
    """Calculate and return technical indicators"""
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    
    if df.empty:
        return {
            "symbol": symbol,
            "interval": interval,
            "error": "No data available",
            "indicators": {}
        }
    
    prices = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    
    rsi = calculate_rsi(prices, period=14)
    macd = calculate_macd(prices)
    
    sma_20 = calculate_sma(prices, 20)
    sma_50 = calculate_sma(prices, 50)
    sma_200 = calculate_sma(prices, 200)
    
    ema_9 = calculate_ema(prices, 9)
    ema_21 = calculate_ema(prices, 21)
    
    bollinger = calculate_bollinger_bands(prices)
    atr = calculate_atr(high, low, prices)
    vwap = calculate_vwap(high, low, prices, volume)
    
    indicators = {
        "rsi": rsi,
        "macd": macd["macd"],
        "macd_signal": macd["signal"],
        "macd_histogram": macd["histogram"],
        "sma_20": sma_20,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "ema_9": ema_9,
        "ema_21": ema_21,
        "bb_upper": bollinger["upper"],
        "bb_middle": bollinger["middle"],
        "bb_lower": bollinger["lower"],
        "atr": atr,
        "vwap": vwap,
        "current_price": round(prices.iloc[-1], 2)
    }
    
    return {
        "symbol": symbol,
        "interval": interval,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "indicators": indicators
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8002"))
    uvicorn.run(app, host="0.0.0.0", port=port)
