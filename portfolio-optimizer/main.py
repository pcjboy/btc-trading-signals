from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import numpy as np
from scipy.optimize import minimize
from datetime import datetime, timezone
import os
import uvicorn
from typing import Dict, List, Any

app = FastAPI(title="Portfolio Optimizer Service")

DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")


async def fetch_ohlcv_data(symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
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


def calculate_returns(prices: pd.Series) -> np.ndarray:
    return prices.pct_change().dropna().values


def portfolio_volatility(weights: np.ndarray, cov_matrix: np.ndarray) -> float:
    return np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))


def portfolio_return(weights: np.ndarray, returns: np.ndarray) -> float:
    return np.sum(returns * weights)


def negative_sharpe(weights: np.ndarray, returns: np.ndarray, cov_matrix: np.ndarray, 
                    risk_free_rate: float = 0.0) -> float:
    port_return = portfolio_return(weights, returns)
    port_vol = portfolio_volatility(weights, cov_matrix)
    if port_vol == 0:
        return 0
    sharpe = (port_return - risk_free_rate) / port_vol
    return -sharpe


def max_sharpe_optimization(expected_returns: np.ndarray, cov_matrix: np.ndarray, 
                           risk_free_rate: float = 0.0) -> Dict[str, Any]:
    n_assets = len(expected_returns)
    initial_weights = np.array([1.0 / n_assets] * n_assets)
    constraints = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}
    bounds = tuple((0, 1) for _ in range(n_assets))
    
    result = minimize(
        negative_sharpe,
        initial_weights,
        args=(expected_returns, cov_matrix, risk_free_rate),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    
    return {
        "weights": result.x.tolist(),
        "expected_return": round(portfolio_return(result.x, expected_returns), 6),
        "volatility": round(portfolio_volatility(result.x, cov_matrix), 6),
        "sharpe_ratio": round(-result.fun, 4) if result.success else 0
    }


def min_volatility_optimization(expected_returns: np.ndarray, cov_matrix: np.ndarray) -> Dict[str, Any]:
    n_assets = len(expected_returns)
    initial_weights = np.array([1.0 / n_assets] * n_assets)
    constraints = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}
    bounds = tuple((0, 1) for _ in range(n_assets))
    
    result = minimize(
        lambda w: portfolio_volatility(w, cov_matrix),
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    
    return {
        "weights": result.x.tolist(),
        "expected_return": round(portfolio_return(result.x, expected_returns), 6),
        "volatility": round(portfolio_volatility(result.x, cov_matrix), 6)
    }


def efficient_return_optimization(expected_returns: np.ndarray, cov_matrix: np.ndarray, 
                                  target_return: float) -> Dict[str, Any]:
    n_assets = len(expected_returns)
    initial_weights = np.array([1.0 / n_assets] * n_assets)
    constraints = [
        {'type': 'eq', 'fun': lambda x: np.sum(x) - 1},
        {'type': 'eq', 'fun': lambda x: portfolio_return(x, expected_returns) - target_return}
    ]
    bounds = tuple((0, 1) for _ in range(n_assets))
    
    result = minimize(
        lambda w: portfolio_volatility(w, cov_matrix),
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    
    return {
        "weights": result.x.tolist(),
        "target_return": target_return,
        "volatility": round(portfolio_volatility(result.x, cov_matrix), 6)
    } if result.success else {}


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "portfolio-optimizer"}


@app.get("/api/v1/portfolio/optimize")
async def optimize_portfolio(
    symbols: str = Query("BTCUSDT,ETHUSDT", description="Comma-separated symbols"),
    interval: str = Query("1h"),
    strategy: str = Query("max_sharpe", description="max_sharpe, min_vol, efficient_return"),
    target_return: float = Query(None, description="Target return for efficient_return strategy")
):
    symbol_list = [s.strip() for s in symbols.split(',')]
    
    price_data = {}
    for symbol in symbol_list:
        df = await fetch_ohlcv_data(symbol, interval, 100)
        if not df.empty:
            price_data[symbol] = df['close']
    
    if len(price_data) < 1:
        return JSONResponse(status_code=400, content={"error": "Insufficient data"})
    
    prices_df = pd.DataFrame(price_data)
    returns = prices_df.pct_change().dropna()
    
    expected_returns = returns.mean().values * 24
    cov_matrix = returns.cov().values * np.sqrt(24)
    
    if strategy == "max_sharpe":
        result = max_sharpe_optimization(expected_returns, cov_matrix)
        strategy_name = "Maximum Sharpe Ratio"
    elif strategy == "min_vol":
        result = min_volatility_optimization(expected_returns, cov_matrix)
        strategy_name = "Minimum Volatility"
    elif strategy == "efficient_return" and target_return:
        result = efficient_return_optimization(expected_returns, cov_matrix, target_return)
        strategy_name = f"Efficient Return ({target_return*100}%)"
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid strategy"})
    
    if not result:
        return JSONResponse(status_code=400, content={"error": "Optimization failed"})
    
    return {
        "symbols": symbol_list,
        "strategy": strategy_name,
        "weights": {s: round(w, 4) for s, w in zip(symbol_list, result.get("weights", []))},
        "expected_return": result.get("expected_return", 0),
        "volatility": result.get("volatility", 0),
        "sharpe_ratio": result.get("sharpe_ratio", 0),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/v1/portfolio/allocation")
async def get_allocation(
    symbols: str = Query("BTCUSDT,ETHUSDT"),
    interval: str = Query("1h"),
    total_capital: float = Query(10000)
):
    symbol_list = [s.strip() for s in symbols.split(',')]
    
    price_data = {}
    for symbol in symbol_list:
        df = await fetch_ohlcv_data(symbol, interval, 100)
        if not df.empty:
            price_data[symbol] = df['close'].iloc[-1]
    
    if len(price_data) < 1:
        return JSONResponse(status_code=400, content={"error": "Insufficient data"})
    
    prices_df = pd.DataFrame(price_data, index=[0]).T
    returns = prices_df.pct_change().dropna()
    
    expected_returns = returns.mean().values * 24
    cov_matrix = returns.cov().values * np.sqrt(24)
    
    result = max_sharpe_optimization(expected_returns, cov_matrix)
    
    weights = result.get("weights", [1.0/len(symbol_list)]*len(symbol_list))
    allocation = {s: round(w * total_capital, 2) for s, w in zip(symbol_list, weights)}
    
    return {
        "symbols": symbol_list,
        "total_capital": total_capital,
        "allocation": allocation,
        "weights": {s: round(w, 4) for s, w in zip(symbol_list, weights)},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/v1/portfolio/efficient-frontier")
async def get_efficient_frontier(
    symbols: str = Query("BTCUSDT,ETHUSDT"),
    interval: str = Query("1h"),
    points: int = Query(10, ge=5, le=50)
):
    symbol_list = [s.strip() for s in symbols.split(',')]
    
    price_data = {}
    for symbol in symbol_list:
        df = await fetch_ohlcv_data(symbol, interval, 100)
        if not df.empty:
            price_data[symbol] = df['close']
    
    prices_df = pd.DataFrame(price_data)
    returns = prices_df.pct_change().dropna()
    
    expected_returns = returns.mean().values * 24
    cov_matrix = returns.cov().values * np.sqrt(24)
    
    min_return = expected_returns.min()
    max_return = expected_returns.max()
    target_returns = np.linspace(min_return, max_return, points)
    
    frontier = []
    for target in target_returns:
        result = efficient_return_optimization(expected_returns, cov_matrix, target)
        if result:
            frontier.append({
                "return": round(target * 100, 2),
                "volatility": round(result["volatility"] * 100, 2),
                "sharpe": round((target - 0.02) / result["volatility"], 2) if result["volatility"] > 0 else 0
            })
    
    return {
        "symbols": symbol_list,
        "efficient_frontier": frontier,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8006"))
    uvicorn.run(app, host="0.0.0.0", port=port)
