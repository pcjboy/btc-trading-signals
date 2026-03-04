from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import uvicorn
from typing import Dict, List, Any

app = FastAPI(title="Order Execution Service")

DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")
SIGNAL_GENERATOR_URL = os.getenv("SIGNAL_GENERATOR_URL", "http://localhost:8003")


async def fetch_current_price(symbol: str) -> float:
    async with httpx.AsyncClient() as client:
        try:
            url = f"{DATA_COLLECTOR_URL}/api/v1/price/current"
            params = {"symbol": symbol}
            response = await client.get(url, params=params)
            data = response.json()
            return data.get("price", 0.0)
        except:
            return 0.0


async def fetch_ohlcv_data(symbol: str, interval: str, limit: int = 50) -> pd.DataFrame:
    async with httpx.AsyncClient() as client:
        try:
            url = f"{DATA_COLLECTOR_URL}/api/v1/ohlcv"
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            response = await client.get(url, params=params)
            data = response.json()
            if not data.get("data"):
                return pd.DataFrame()
            df = pd.DataFrame(data["data"])
            if not df.empty:
                df = df.sort_values("timestamp").reset_index(drop=True)
            return df
        except:
            return pd.DataFrame()


def calculate_twap_price(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    return round(typical_price.mean(), 2)


def calculate_vwap_price(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    vwap = (typical_price * df['volume']).sum() / df['volume'].sum()
    return round(vwap, 2)


def calculate_vwap_execution(order_size: float, df: pd.DataFrame, slice_count: int = 10) -> Dict[str, Any]:
    if df.empty or order_size <= 0:
        return {}
    
    slice_size = order_size / slice_count
    executed = 0
    total_cost = 0
    
    df['slice'] = pd.cut(range(len(df)), bins=slice_count, labels=range(slice_count))
    
    for i in range(slice_count):
        slice_data = df[df['slice'] == i]
        if len(slice_data) == 0:
            continue
        execution_price = (slice_data['high'] + slice_data['low'] + slice_data['close']).mean() / 3
        slice_executed = min(slice_size, order_size - executed)
        total_cost += slice_executed * execution_price
        executed += slice_executed
    
    avg_price = total_cost / executed if executed > 0 else 0
    slippage = ((avg_price - df['close'].iloc[-1]) / df['close'].iloc[-1]) * 100 if executed > 0 else 0
    
    return {
        "executed_amount": round(executed, 4),
        "avg_price": round(avg_price, 2),
        "slippage_percentage": round(slippage, 4)
    }


def calculate_twap_execution(order_size: float, df: pd.DataFrame, slice_count: int = 10) -> Dict[str, Any]:
    if df.empty or order_size <= 0:
        return {}
    
    slice_size = order_size / slice_count
    executed = 0
    total_cost = 0
    
    interval_size = len(df) // slice_count
    for i in range(slice_count):
        start_idx = i * interval_size
        end_idx = (i + 1) * interval_size if i < slice_count - 1 else len(df)
        slice_data = df.iloc[start_idx:end_idx]
        if len(slice_data) == 0:
            continue
        execution_price = (slice_data['high'] + slice_data['low'] + slice_data['close']).mean() / 3
        slice_executed = min(slice_size, order_size - executed)
        total_cost += slice_executed * execution_price
        executed += slice_executed
    
    avg_price = total_cost / executed if executed > 0 else 0
    slippage = ((avg_price - df['close'].iloc[-1]) / df['close'].iloc[-1]) * 100 if executed > 0 else 0
    
    return {
        "executed_amount": round(executed, 4),
        "avg_price": round(avg_price, 2),
        "slippage_percentage": round(slippage, 4)
    }


def calculate_pov_execution(order_size: float, df: pd.DataFrame, target_pct: float = 0.2) -> Dict[str, Any]:
    if df.empty or order_size <= 0:
        return {}
    
    total_volume = df['volume'].sum()
    target_volume = total_volume * target_pct
    
    executed = 0
    total_cost = 0
    
    for _, row in df.iterrows():
        slice_volume = row['volume'] * target_pct
        execution_price = (row['high'] + row['low'] + row['close']) / 3
        slice_executed = min(slice_volume, order_size - executed)
        total_cost += slice_executed * execution_price
        executed += slice_executed
        if executed >= order_size:
            break
    
    avg_price = total_cost / executed if executed > 0 else 0
    slippage = ((avg_price - df['close'].iloc[-1]) / df['close'].iloc[-1]) * 100 if executed > 0 else 0
    
    return {
        "executed_amount": round(executed, 4),
        "avg_price": round(avg_price, 2),
        "slippage_percentage": round(slippage, 4)
    }


def calculate_adaptive_execution(order_size: float, df: pd.DataFrame, urgency: str = "medium") -> Dict[str, Any]:
    if df.empty or order_size <= 0:
        return {}
    
    volatility = df['close'].pct_change().std()
    
    if urgency == "low":
        slice_count = 20
    elif urgency == "medium":
        slice_count = 10
    else:
        slice_count = 5
    
    urgency_multiplier = {"low": 0.5, "medium": 1.0, "high": 1.5}.get(urgency, 1.0)
    
    slice_size = order_size / slice_count
    executed = 0
    total_cost = 0
    
    interval_size = len(df) // slice_count
    for i in range(slice_count):
        start_idx = i * interval_size
        end_idx = (i + 1) * interval_size if i < slice_count - 1 else len(df)
        slice_data = df.iloc[start_idx:end_idx]
        if len(slice_data) == 0:
            continue
        
        base_price = (slice_data['high'] + slice_data['low'] + slice_data['close']).mean() / 3
        volatility_adjustment = 1 + (volatility * urgency_multiplier * (i / slice_count))
        execution_price = base_price * volatility_adjustment
        
        slice_executed = min(slice_size, order_size - executed)
        total_cost += slice_executed * execution_price
        executed += slice_executed
    
    avg_price = total_cost / executed if executed > 0 else 0
    slippage = ((avg_price - df['close'].iloc[-1]) / df['close'].iloc[-1]) * 100 if executed > 0 else 0
    
    return {
        "executed_amount": round(executed, 4),
        "avg_price": round(avg_price, 2),
        "slippage_percentage": round(slippage, 4),
        "urgency": urgency,
        "slices": slice_count
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "order-executor"}


@app.get("/api/v1/order/execute")
async def execute_order(
    symbol: str = Query("BTCUSDT"),
    side: str = Query("BUY", description="BUY or SELL"),
    order_type: str = Query("market", description="market, limit, twap, vwap, pov, adaptive"),
    quantity: float = Query(..., description="Order quantity"),
    limit_price: float = Query(None, description="Limit price for limit orders"),
    strategy_params: str = Query("", description="Strategy specific parameters (JSON)")
):
    current_price = await fetch_current_price(symbol)
    if current_price <= 0:
        return JSONResponse(status_code=400, content={"error": "Cannot fetch current price"})
    
    execution_strategy = {
        "market": {
            "price": current_price,
            "description": "Immediate execution at current market price"
        },
        "limit": {
            "price": limit_price if limit_price else current_price,
            "description": f"Execution at limit price {limit_price}"
        },
        "twap": {
            "description": "Time-Weighted Average Price execution",
            "slices": 10
        },
        "vwap": {
            "description": "Volume-Weighted Average Price execution",
            "slices": 10
        },
        "pov": {
            "description": "Percentage of Volume execution",
            "target_pct": 0.2
        },
        "adaptive": {
            "description": "Adaptive execution based on volatility",
            "urgency": "medium"
        }
    }
    
    order_result = {
        "order_id": f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "status": "PENDING",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    if order_type == "market":
        order_result["execution_price"] = current_price
        order_result["status"] = "FILLED"
    elif order_type == "limit" and limit_price:
        if (side == "BUY" and current_price <= limit_price) or (side == "SELL" and current_price >= limit_price):
            order_result["execution_price"] = limit_price
            order_result["status"] = "FILLED"
        else:
            order_result["execution_price"] = limit_price
            order_result["status"] = "PENDING"
    else:
        df = await fetch_ohlcv_data(symbol, "5m", 50)
        
        if order_type == "twap":
            result = calculate_twap_execution(quantity, df)
        elif order_type == "vwap":
            result = calculate_vwap_execution(quantity, df)
        elif order_type == "pov":
            result = calculate_pov_execution(quantity, df)
        elif order_type == "adaptive":
            result = calculate_adaptive_execution(quantity, df)
        else:
            result = {}
        
        if result:
            order_result["execution_price"] = result.get("avg_price", current_price)
            order_result["status"] = "FILLED"
            order_result["execution_details"] = result
    
    return order_result


@app.get("/api/v1/order/estimate")
async def estimate_execution(
    symbol: str = Query("BTCUSDT"),
    order_type: str = Query("twap"),
    quantity: float = Query(1.0),
    urgency: str = Query("medium")
):
    df = await fetch_ohlcv_data(symbol, "5m", 50)
    if df.empty:
        return JSONResponse(status_code=400, content={"error": "Insufficient data"})
    
    current_price = df['close'].iloc[-1]
    
    if order_type == "twap":
        estimate = calculate_twap_execution(quantity, df)
        strategy = "TWAP"
    elif order_type == "vwap":
        estimate = calculate_vwap_execution(quantity, df)
        strategy = "VWAP"
    elif order_type == "pov":
        estimate = calculate_pov_execution(quantity, df)
        strategy = "POV"
    elif order_type == "adaptive":
        estimate = calculate_adaptive_execution(quantity, df, urgency)
        strategy = "Adaptive"
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid order type"})
    
    return {
        "symbol": symbol,
        "strategy": strategy,
        "quantity": quantity,
        "current_price": current_price,
        "estimated_price": estimate.get("avg_price", current_price),
        "estimated_slippage": estimate.get("slippage_percentage", 0),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/v1/order/strategies")
async def get_strategies():
    return {
        "strategies": [
            {
                "name": "market",
                "description": "Immediate market execution",
                "pros": ["Guaranteed execution", "Low latency"],
                "cons": ["Higher slippage in volatile markets"]
            },
            {
                "name": "limit",
                "description": "Limit order execution",
                "pros": ["Price control", "No slippage if filled"],
                "cons": ["May not execute"]
            },
            {
                "name": "twap",
                "description": "Time-Weighted Average Price",
                "pros": ["Reduced market impact", "Predictable execution"],
                "cons": ["Exposure to price movement"]
            },
            {
                "name": "vwap",
                "description": "Volume-Weighted Average Price",
                "pros": ["Better than TWAP in liquid markets", "Natural execution"],
                "cons": ["Requires volume data"]
            },
            {
                "name": "pov",
                "description": "Percentage of Volume",
                "pros": ["Market-aware execution", "Flexible"],
                "cons": ["Dependent on volume forecast"]
            },
            {
                "name": "adaptive",
                "description": "Adaptive execution based on volatility",
                "pros": ["Dynamic adjustment", "Risk-aware"],
                "cons": ["Complex parameter tuning"]
            }
        ]
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8007"))
    uvicorn.run(app, host="0.0.0.0", port=port)
