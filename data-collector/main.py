from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import sqlite3
from typing import Optional, List, Dict, Any
import uvicorn

app = FastAPI(title="Data Collector Service")

BINANCE_API_URL = "https://api.binance.com/api/v3"
DB_PATH = "/app/data/btc_data.db"

os.makedirs("/app/data", exist_ok=True)


def init_db():
    """Initialize SQLite database for OHLCV data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            interval TEXT NOT NULL,
            UNIQUE(symbol, timestamp, interval)
        )
    """)
    conn.commit()
    conn.close()


init_db()


async def fetch_ohlcv_from_binance(symbol: str, interval: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Fetch OHLCV data from Binance API"""
    async with httpx.AsyncClient() as client:
        try:
            url = f"{BINANCE_API_URL}/klines"
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
            response = await client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            ohlcv_data = []
            for kline in data:
                ohlcv_data.append({
                    "symbol": symbol,
                    "timestamp": kline[0],
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                    "interval": interval,
                    "datetime": datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc).isoformat()
                })
            
            return ohlcv_data
        except Exception as e:
            print(f"Error fetching data from Binance: {e}")
            return []


async def get_current_price_from_binance(symbol: str) -> float:
    """Get current BTC price from Binance"""
    async with httpx.AsyncClient() as client:
        try:
            url = f"{BINANCE_API_URL}/ticker/price"
            params = {"symbol": symbol}
            response = await client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return float(data["price"])
        except Exception as e:
            print(f"Error fetching current price: {e}")
            return 0.0


def save_ohlcv_to_db(ohlcv_data: List[Dict[str, Any]]):
    """Save OHLCV data to SQLite database"""
    if not ohlcv_data:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for item in ohlcv_data:
        cursor.execute("""
            INSERT OR REPLACE INTO ohlcv 
            (symbol, timestamp, open, high, low, close, volume, interval)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item["symbol"],
            item["timestamp"],
            item["open"],
            item["high"],
            item["low"],
            item["close"],
            item["volume"],
            item["interval"]
        ))
    
    conn.commit()
    conn.close()


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "data-collector"}


@app.get("/api/v1/ohlcv")
async def get_ohlcv(
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval (1m, 5m, 15m, 1h, 4h, 1d)"),
    limit: int = Query(100, description="Number of candles", ge=1, le=1000)
):
    """Get OHLCV data for a symbol"""
    ohlcv_data = await fetch_ohlcv_from_binance(symbol, interval, limit)
    
    if ohlcv_data:
        save_ohlcv_to_db(ohlcv_data)
    
    return {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "count": len(ohlcv_data),
        "data": ohlcv_data
    }


@app.get("/api/v1/price/current")
async def get_current_price(
    symbol: str = Query("BTCUSDT", description="Trading symbol")
):
    """Get current price for a symbol"""
    price = await get_current_price_from_binance(symbol)
    
    return {
        "symbol": symbol,
        "price": price,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/v1/price/history")
async def get_price_history(
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval"),
    limit: int = Query(100, description="Number of candles")
):
    """Get historical price data"""
    ohlcv_data = await fetch_ohlcv_from_binance(symbol, interval, limit)
    
    return {
        "symbol": symbol,
        "interval": interval,
        "data": [{"timestamp": item["timestamp"], "close": item["close"]} for item in ohlcv_data]
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
