from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import uvicorn
from typing import Dict, List, Any, Optional

app = FastAPI(title="Risk Model Service")

DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")


async def fetch_ohlcv_data(symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
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
                df = df.sort_values("timestamp").reset_index(drop=True)
            return df
        except Exception as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame()


def calculate_var(returns: pd.Series, confidence: float = 0.95) -> float:
    try:
        var = np.percentile(returns.dropna(), (1 - confidence) * 100)
        return round(var, 6)
    except:
        return 0.0


def calculate_cvar(returns: pd.Series, confidence: float = 0.95) -> float:
    try:
        var = calculate_var(returns, confidence)
        cvar = returns[returns <= var].mean()
        return round(cvar, 6) if not pd.isna(cvar) else var
    except:
        return 0.0


def calculate_max_drawdown(prices: pd.Series) -> Dict[str, float]:
    try:
        rolling_max = prices.expanding().max()
        drawdown = (prices - rolling_max) / rolling_max
        max_dd = drawdown.min()
        return {
            "max_drawdown": round(max_dd, 6),
            "current_drawdown": round(drawdown.iloc[-1], 6)
        }
    except:
        return {"max_drawdown": 0.0, "current_drawdown": 0.0}


def calculate_volatility(returns: pd.Series, annualize: bool = True) -> float:
    try:
        vol = returns.std()
        if annualize:
            vol = vol * np.sqrt(252)
        return round(vol, 6)
    except:
        return 0.0


def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    try:
        excess_returns = returns - risk_free_rate / 252
        if excess_returns.std() == 0:
            return 0.0
        sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(252)
        return round(sharpe, 4)
    except:
        return 0.0


def calculate_sortino_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    try:
        excess_returns = returns - risk_free_rate / 252
        downside_returns = excess_returns[excess_returns < 0]
        if len(downside_returns) == 0 or downside_returns.std() == 0:
            return 0.0
        sortino = excess_returns.mean() / downside_returns.std() * np.sqrt(252)
        return round(sortino, 4)
    except:
        return 0.0


def calculate_calmar_ratio(returns: pd.Series, prices: pd.Series) -> float:
    try:
        annual_return = returns.mean() * 252
        dd_info = calculate_max_drawdown(prices)
        max_dd = abs(dd_info["max_drawdown"])
        if max_dd == 0:
            return 0.0
        calmar = annual_return / max_dd
        return round(calmar, 4)
    except:
        return 0.0


def calculate_beta(returns: pd.Series, benchmark_returns: pd.Series) -> float:
    try:
        covariance = returns.cov(benchmark_returns)
        benchmark_variance = benchmark_returns.var()
        if benchmark_variance == 0:
            return 1.0
        beta = covariance / benchmark_variance
        return round(beta, 4)
    except:
        return 1.0


def calculate_correlation(returns: pd.Series, other_returns: pd.Series) -> float:
    try:
        corr = returns.corr(other_returns)
        return round(corr, 4)
    except:
        return 0.0


def calculate_skewness(returns: pd.Series) -> float:
    try:
        skew = returns.skew()
        return round(skew, 4)
    except:
        return 0.0


def calculate_kurtosis(returns: pd.Series) -> float:
    try:
        kurt = returns.kurt()
        return round(kurt, 4)
    except:
        return 0.0


def calculate_value_at_risk_mc(returns: pd.Series, initial_investment: float = 10000, 
                               confidence: float = 0.95, simulations: int = 10000) -> Dict[str, float]:
    try:
        mu = returns.mean()
        sigma = returns.std()
        simulated_returns = np.random.normal(mu, sigma, simulations)
        portfolio_values = initial_investment * (1 + simulated_returns)
        var_percentile = np.percentile(portfolio_values, (1 - confidence) * 100)
        var_loss = initial_investment - var_percentile
        return {
            "var_absolute": round(var_loss, 2),
            "var_percentage": round(var_loss / initial_investment * 100, 2),
            "expected_loss": round(np.mean(initial_investment - portfolio_values[portfolio_values < initial_investment]), 2)
        }
    except:
        return {"var_absolute": 0.0, "var_percentage": 0.0, "expected_loss": 0.0}


def calculate_risk_score(risk_metrics: Dict[str, float]) -> float:
    score = 0.0
    weights = {
        "var_95": 0.25,
        "max_drawdown": 0.20,
        "volatility": 0.20,
        "beta": 0.15,
        "sortino_ratio": 0.20
    }
    var_score = min(risk_metrics.get("var_95", 0) * 10, 1.0)
    dd_score = min(abs(risk_metrics.get("max_drawdown", 0)), 1.0)
    vol_score = min(risk_metrics.get("volatility", 0) / 0.5, 1.0)
    beta_score = abs(risk_metrics.get("beta", 1.0) - 1.0)
    sortino_inv = 1 - min(max(risk_metrics.get("sortino_ratio", 0) / 3, 0), 1)
    
    score = (var_score * weights["var_95"] + 
             dd_score * weights["max_drawdown"] + 
             vol_score * weights["volatility"] +
             beta_score * weights["beta"] +
             sortino_inv * weights["sortino_ratio"])
    
    return round(min(score, 1.0), 4)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "risk-model"}


@app.get("/api/v1/risk/full")
async def get_full_risk_analysis(
    symbol: str = Query("BTCUSDT"),
    interval: str = Query("1h"),
    initial_investment: float = Query(10000, description="Initial investment for VaR")
):
    df = await fetch_ohlcv_data(symbol, interval, limit=500)
    
    if df.empty:
        return JSONResponse(status_code=400, content={"error": "No data available"})
    
    prices = df['close']
    returns = df['close'].pct_change().dropna()
    
    risk_metrics = {
        "var_95": calculate_var(returns, 0.95),
        "var_99": calculate_var(returns, 0.99),
        "cvar_95": calculate_cvar(returns, 0.95),
        "volatility_daily": calculate_volatility(returns, annualize=False),
        "volatility_annual": calculate_volatility(returns, annualize=True),
        "sharpe_ratio": calculate_sharpe_ratio(returns),
        "sortino_ratio": calculate_sortino_ratio(returns),
        "calmar_ratio": calculate_calmar_ratio(returns, prices),
        "skewness": calculate_skewness(returns),
        "kurtosis": calculate_kurtosis(returns),
    }
    
    dd_info = calculate_max_drawdown(prices)
    risk_metrics.update(dd_info)
    
    var_mc = calculate_value_at_risk_mc(returns, initial_investment)
    risk_metrics["var_mc"] = var_mc
    
    risk_metrics["risk_score"] = calculate_risk_score(risk_metrics)
    
    return {
        "symbol": symbol,
        "interval": interval,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "risk_metrics": risk_metrics,
        "risk_level": "VERY_HIGH" if risk_metrics["risk_score"] > 0.7 else
                      "HIGH" if risk_metrics["risk_score"] > 0.5 else
                      "MEDIUM" if risk_metrics["risk_score"] > 0.3 else "LOW"
    }


@app.get("/api/v1/risk/var")
async def get_value_at_risk(
    symbol: str = Query("BTCUSDT"),
    interval: str = Query("1h"),
    confidence: float = Query(0.95, ge=0.9, le=0.99)
):
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    if df.empty:
        return JSONResponse(status_code=400, content={"error": "No data available"})
    
    returns = df['close'].pct_change().dropna()
    var = calculate_var(returns, confidence)
    cvar = calculate_cvar(returns, confidence)
    
    return {
        "symbol": symbol,
        "confidence_level": confidence,
        "var": var,
        "cvar": cvar,
        "interpretation": f"With {confidence*100}% confidence, maximum expected loss is {abs(var)*100:.2f}%"
    }


@app.get("/api/v1/risk/drawdown")
async def get_drawdown(
    symbol: str = Query("BTCUSDT"),
    interval: str = Query("1h")
):
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    if df.empty:
        return JSONResponse(status_code=400, content={"error": "No data available"})
    
    prices = df['close']
    dd_info = calculate_max_drawdown(prices)
    
    return {
        "symbol": symbol,
        "current_price": prices.iloc[-1],
        "max_drawdown": dd_info["max_drawdown"],
        "current_drawdown": dd_info["current_drawdown"],
        "risk_level": "HIGH" if dd_info["current_drawdown"] < -0.2 else 
                      "MEDIUM" if dd_info["current_drawdown"] < -0.1 else "LOW"
    }


@app.get("/api/v1/risk/performance")
async def get_performance_metrics(
    symbol: str = Query("BTCUSDT"),
    interval: str = Query("1h")
):
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    if df.empty:
        return JSONResponse(status_code=400, content={"error": "No data available"})
    
    prices = df['close']
    returns = df['close'].pct_change().dropna()
    
    return {
        "symbol": symbol,
        "sharpe_ratio": calculate_sharpe_ratio(returns),
        "sortino_ratio": calculate_sortino_ratio(returns),
        "calmar_ratio": calculate_calmar_ratio(returns, prices),
        "volatility_annual": calculate_volatility(returns, annualize=True),
        "skewness": calculate_skewness(returns),
        "kurtosis": calculate_kurtosis(returns),
        "total_return": round(returns.sum(), 4),
        "avg_daily_return": round(returns.mean(), 6)
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8005"))
    uvicorn.run(app, host="0.0.0.0", port=port)
