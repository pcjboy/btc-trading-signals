from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import os
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import uuid
import uvicorn

app = FastAPI(title="Signal Generator Service")

ANALYZER_URL = os.getenv("ANALYZER_URL", "http://localhost:8002")
DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")
DB_PATH = "/app/data/signals.db"

os.makedirs("/app/data", exist_ok=True)


def init_db():
    """Initialize SQLite database for signals"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            confidence REAL NOT NULL,
            entry_price REAL NOT NULL,
            take_profit REAL NOT NULL,
            stop_loss REAL NOT NULL,
            risk_reward_ratio REAL NOT NULL,
            reason TEXT,
            interval TEXT
        )
    """)
    conn.commit()
    conn.close()


init_db()


async def fetch_indicators(symbol: str, interval: str) -> Dict[str, Any]:
    """Fetch indicators from Analyzer service"""
    async with httpx.AsyncClient() as client:
        try:
            url = f"{ANALYZER_URL}/api/v1/analyze/indicators"
            params = {"symbol": symbol, "interval": interval}
            response = await client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("indicators", {})
        except Exception as e:
            print(f"Error fetching indicators: {e}")
            return {}


async def fetch_current_price(symbol: str) -> float:
    """Fetch current price from Data Collector"""
    async with httpx.AsyncClient() as client:
        try:
            url = f"{DATA_COLLECTOR_URL}/api/v1/price/current"
            params = {"symbol": symbol}
            response = await client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("price", 0.0)
        except Exception as e:
            print(f"Error fetching price: {e}")
            return 0.0


def generate_signal(indicators: Dict[str, Any], current_price: float) -> Dict[str, Any]:
    """Generate trading signal based on indicators"""
    if not indicators or current_price == 0:
        return {
            "action": "HOLD",
            "confidence": 0.0,
            "reason": "Insufficient data"
        }
    
    signals = []
    reasons = []
    
    rsi = indicators.get("rsi", 50)
    macd = indicators.get("macd", 0)
    macd_signal = indicators.get("macd_signal", 0)
    macd_histogram = indicators.get("macd_histogram", 0)
    sma_20 = indicators.get("sma_20", 0)
    sma_50 = indicators.get("sma_50", 0)
    sma_200 = indicators.get("sma_200", 0)
    bb_upper = indicators.get("bb_upper", 0)
    bb_lower = indicators.get("bb_lower", 0)
    atr = indicators.get("atr", 0)
    
    if rsi < 30:
        signals.append("BUY")
        reasons.append(f"RSI oversold: {rsi}")
    elif rsi > 70:
        signals.append("SELL")
        reasons.append(f"RSI overbought: {rsi}")
    
    if macd > macd_signal and macd_histogram > 0:
        signals.append("BUY")
        reasons.append("MACD golden cross")
    elif macd < macd_signal and macd_histogram < 0:
        signals.append("SELL")
        reasons.append("MACD death cross")
    
    if sma_50 > sma_200 and sma_20 > sma_50:
        signals.append("BUY")
        reasons.append("Golden cross (50 SMA > 200 SMA)")
    elif sma_50 < sma_200 and sma_20 < sma_50:
        signals.append("SELL")
        reasons.append("Death cross (50 SMA < 200 SMA)")
    
    if current_price <= bb_lower:
        signals.append("BUY")
        reasons.append("Price at lower Bollinger Band")
    elif current_price >= bb_upper:
        signals.append("SELL")
        reasons.append("Price at upper Bollinger Band")
    
    buy_count = signals.count("BUY")
    sell_count = signals.count("SELL")
    
    if buy_count > sell_count:
        action = "BUY"
        confidence = min(buy_count / 4.0, 1.0)
    elif sell_count > buy_count:
        action = "SELL"
        confidence = min(sell_count / 4.0, 1.0)
    else:
        action = "HOLD"
        confidence = 0.3
    
    if action == "HOLD" and rsi < 40:
        action = "BUY"
        confidence = 0.5
        reasons.append("RSI trending toward oversold")
    elif action == "HOLD" and rsi > 60:
        action = "SELL"
        confidence = 0.5
        reasons.append("RSI trending toward overbought")
    
    return {
        "action": action,
        "confidence": round(confidence, 2),
        "reasons": reasons
    }


def calculate_take_profit_stop_loss(
    action: str,
    entry_price: float,
    atr: float,
    bb_upper: float,
    bb_lower: float
) -> Dict[str, float]:
    """Calculate take profit and stop loss levels"""
    if action == "HOLD" or entry_price == 0:
        return {
            "take_profit": entry_price,
            "stop_loss": entry_price,
            "risk_reward_ratio": 0.0
        }
    
    if action == "BUY":
        tp_atr = entry_price + (2 * atr)
        tp_bb = bb_upper if bb_upper > 0 else entry_price * 1.05
        tp_fixed = entry_price * 1.05
        
        take_profit = min(tp_atr, tp_bb, tp_fixed)
        
        sl_atr = entry_price - (1.5 * atr)
        sl_bb = bb_lower * 0.99 if bb_lower > 0 else entry_price * 0.975
        sl_fixed = entry_price * 0.975
        
        stop_loss = max(sl_atr, sl_bb, sl_fixed)
        
    else:
        tp_atr = entry_price - (2 * atr)
        tp_bb = bb_lower if bb_lower > 0 else entry_price * 0.95
        tp_fixed = entry_price * 0.95
        
        take_profit = max(tp_atr, tp_bb, tp_fixed)
        
        sl_atr = entry_price + (1.5 * atr)
        sl_bb = bb_upper * 1.01 if bb_upper > 0 else entry_price * 1.025
        sl_fixed = entry_price * 1.025
        
        stop_loss = min(sl_atr, sl_bb, sl_fixed)
    
    risk = abs(entry_price - stop_loss)
    reward = abs(take_profit - entry_price)
    risk_reward = round(reward / risk, 2) if risk > 0 else 0.0
    
    return {
        "take_profit": round(take_profit, 2),
        "stop_loss": round(stop_loss, 2),
        "risk_reward_ratio": risk_reward
    }


def save_signal_to_db(signal_data: Dict[str, Any]):
    """Save signal to database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO signals 
        (id, symbol, timestamp, action, confidence, entry_price, 
         take_profit, stop_loss, risk_reward_ratio, reason, interval)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        signal_data["id"],
        signal_data["symbol"],
        signal_data["timestamp"],
        signal_data["action"],
        signal_data["confidence"],
        signal_data["entry_price"],
        signal_data["take_profit"],
        signal_data["stop_loss"],
        signal_data["risk_reward_ratio"],
        signal_data.get("reason", ""),
        signal_data.get("interval", "1h")
    ))
    
    conn.commit()
    conn.close()


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "signal-generator"}


@app.get("/api/v1/generate/signals")
async def generate_signals(
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval")
):
    """Generate trading signals"""
    indicators = await fetch_indicators(symbol, interval)
    current_price = await fetch_current_price(symbol)
    
    signal_info = generate_signal(indicators, current_price)
    action = signal_info["action"]
    confidence = signal_info["confidence"]
    
    tp_sl = calculate_take_profit_stop_loss(
        action,
        current_price,
        indicators.get("atr", 0),
        indicators.get("bb_upper", 0),
        indicators.get("bb_lower", 0)
    )
    
    signal_data = {
        "id": str(uuid.uuid4()),
        "symbol": symbol,
        "interval": interval,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "confidence": confidence,
        "entry_price": current_price,
        "take_profit": tp_sl["take_profit"],
        "stop_loss": tp_sl["stop_loss"],
        "risk_reward_ratio": tp_sl["risk_reward_ratio"],
        "reason": ", ".join(signal_info.get("reasons", [])) if signal_info.get("reasons") else "No clear signal"
    }
    
    if action != "HOLD":
        save_signal_to_db(signal_data)
    
    return {
        "symbol": symbol,
        "interval": interval,
        "timestamp": signal_data["timestamp"],
        "signal": {
            "action": signal_data["action"],
            "confidence": signal_data["confidence"],
            "entry_price": signal_data["entry_price"],
            "take_profit": signal_data["take_profit"],
            "stop_loss": signal_data["stop_loss"],
            "risk_reward_ratio": signal_data["risk_reward_ratio"],
            "reason": signal_data["reason"]
        },
        "indicators": indicators
    }


@app.get("/api/v1/signals/history")
async def get_signals_history(
    limit: int = Query(100, description="Number of signals to return"),
    offset: int = Query(0, description="Offset for pagination")
):
    """Get historical signals"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, symbol, timestamp, action, confidence, entry_price,
               take_profit, stop_loss, risk_reward_ratio, reason, interval
        FROM signals
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))
    
    rows = cursor.fetchall()
    conn.close()
    
    signals = []
    for row in rows:
        signals.append({
            "id": row[0],
            "symbol": row[1],
            "timestamp": row[2],
            "action": row[3],
            "confidence": row[4],
            "entry_price": row[5],
            "take_profit": row[6],
            "stop_loss": row[7],
            "risk_reward_ratio": row[8],
            "reason": row[9],
            "interval": row[10]
        })
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM signals")
    total = cursor.fetchone()[0]
    conn.close()
    
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": signals
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8003"))
    uvicorn.run(app, host="0.0.0.0", port=port)
