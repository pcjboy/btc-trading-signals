"""
Alpha Factor Mining Service
挖掘和计算Alpha因子，用于预测资产收益
"""

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import os
import uvicorn
from typing import Dict, List, Any, Optional

app = FastAPI(title="Alpha Miner Service")

DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")
ANALYZER_URL = os.getenv("ANALYZER_URL", "http://localhost:8002")


async def fetch_ohlcv_data(symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
    """Fetch OHLCV data from Data Collector"""
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


# ============ Alpha Factor Functions ============

def alpha_001(returns: pd.Series, period: int = 5) -> float:
    """Alpha #001: 短期动量 (5日累计收益)"""
    try:
        cum_returns = returns.rolling(window=period).sum()
        return round(cum_returns.iloc[-1], 6)
    except:
        return 0.0


def alpha_002(returns: pd.Series, period: int = 20) -> float:
    """Alpha #002: 中期动量 (20日累计收益)"""
    try:
        cum_returns = returns.rolling(window=period).sum()
        return round(cum_returns.iloc[-1], 6)
    except:
        return 0.0


def alpha_003(returns: pd.Series, period: int = 60) -> float:
    """Alpha #003: 长期动量 (60日累计收益)"""
    try:
        cum_returns = returns.rolling(window=period).sum()
        return round(cum_returns.iloc[-1], 6)
    except:
        return 0.0


def alpha_004(volume: pd.Series, period: int = 5) -> float:
    """Alpha #004: 成交量动量 (成交量变化率)"""
    try:
        vol_ma = volume.rolling(window=period).mean()
        volume_ratio = volume / vol_ma
        return round(volume_ratio.iloc[-1], 4)
    except:
        return 1.0


def alpha_005(prices: pd.Series, period: int = 20) -> float:
    """Alpha #005: 价格位置 (当前价格在N日区间的位置)"""
    try:
        highest = prices.rolling(window=period).max()
        lowest = prices.rolling(window=period).min()
        position = (prices - lowest) / (highest - lowest)
        return round(position.iloc[-1], 4)
    except:
        return 0.5


def alpha_006(returns: pd.Series, period: int = 20) -> float:
    """Alpha #006: 收益波动率 (N日收益标准差)"""
    try:
        vol = returns.rolling(window=period).std()
        return round(vol.iloc[-1], 6)
    except:
        return 0.0


def alpha_007(prices: pd.Series, period: int = 10) -> float:
    """Alpha #007: 价格加速度 (价格变化率的变化)"""
    try:
        price_change = prices.pct_change()
        acceleration = price_change.pct_change()
        return round(acceleration.iloc[-1], 6)
    except:
        return 0.0


def alpha_008(volume: pd.Series, prices: pd.Series, period: int = 20) -> float:
    """Alpha #008: 成交量加权价格趋势"""
    try:
        vwap = (prices * volume).rolling(window=period).sum() / volume.rolling(window=period).sum()
        price_vwap_ratio = prices / vwap - 1
        return round(price_vwap_ratio.iloc[-1], 6)
    except:
        return 0.0


def alpha_009(prices: pd.Series, period: int = 20) -> float:
    """Alpha #009: 移动平均线偏离度"""
    try:
        sma = prices.rolling(window=period).mean()
        deviation = (prices - sma) / sma
        return round(deviation.iloc[-1], 6)
    except:
        return 0.0


def alpha_010(returns: pd.Series, period: int = 20) -> float:
    """Alpha #010: 收益偏度 (收益分布偏度)"""
    try:
        skew = returns.rolling(window=period).skew()
        return round(skew.iloc[-1], 4)
    except:
        return 0.0


def alpha_011(returns: pd.Series, period: int = 20) -> float:
    """Alpha #011: 收益峰度 (收益分布峰度)"""
    try:
        kurt = returns.rolling(window=period).kurt()
        return round(kurt.iloc[-1], 4)
    except:
        return 0.0


def alpha_012(prices: pd.Series, volume: pd.Series, period: int = 20) -> float:
    """Alpha #012: 资金流指标 (价格*成交量的变化)"""
    try:
        money_flow = (prices * volume).pct_change()
        cum_flow = money_flow.rolling(window=period).sum()
        return round(cum_flow.iloc[-1], 6)
    except:
        return 0.0


def alpha_013(prices: pd.Series, period: int = 20) -> float:
    """Alpha #013: 价格突破 (当前价格相对于N日高点的位置)"""
    try:
        highest = prices.rolling(window=period).max()
        breakthrough = (prices - highest.shift(1)) / highest.shift(1)
        return round(breakthrough.iloc[-1], 6)
    except:
        return 0.0


def alpha_014(prices: pd.Series, period: int = 20) -> float:
    """Alpha #014: 布林带位置"""
    try:
        sma = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        bb_upper = sma + 2 * std
        bb_lower = sma - 2 * std
        bb_position = (prices - bb_lower) / (bb_upper - bb_lower)
        return round(bb_position.iloc[-1], 4)
    except:
        return 0.5


def alpha_015(returns: pd.Series, period: int = 10) -> float:
    """Alpha #015: 收益趋势强度"""
    try:
        pos_returns = (returns > 0).rolling(window=period).sum()
        trend_strength = pos_returns / period
        return round(trend_strength.iloc[-1], 4)
    except:
        return 0.5


def alpha_016(prices: pd.Series, period: int = 20) -> float:
    """Alpha #016: 均线金叉/死叉信号"""
    try:
        sma_short = prices.rolling(window=5).mean()
        sma_long = prices.rolling(window=period).mean()
        signal = (sma_short - sma_long) / sma_long
        return round(signal.iloc[-1], 6)
    except:
        return 0.0


def alpha_017(volume: pd.Series, period: int = 20) -> float:
    """Alpha #017: 成交量异常检测"""
    try:
        vol_ma = volume.rolling(window=period).mean()
        vol_std = volume.rolling(window=period).std()
        anomaly = (volume - vol_ma) / vol_std
        return round(anomaly.iloc[-1], 4)
    except:
        return 0.0


def alpha_018(prices: pd.Series, period: int = 20) -> float:
    """Alpha #018: 价格波动率 (N日收益的年化标准差)"""
    try:
        daily_vol = returns.std()
        annualized_vol = daily_vol * np.sqrt(252)
        return round(annualized_vol, 6)
    except:
        return 0.0


def alpha_019(prices: pd.Series, period: int = 20) -> float:
    """Alpha #019: 价格回调 (从高点的回撤)"""
    try:
        rolling_max = prices.rolling(window=period).max()
        drawdown = (prices - rolling_max) / rolling_max
        return round(drawdown.iloc[-1], 6)
    except:
        return 0.0


def alpha_020(prices: pd.Series, returns: pd.Series, period: int = 20) -> float:
    """Alpha #020: 风险调整收益 (夏普比率近似)"""
    try:
        mean_return = returns.rolling(window=period).mean()
        vol = returns.rolling(window=period).std()
        sharpe = mean_return / vol * np.sqrt(252) if vol.iloc[-1] > 0 else 0
        return round(sharpe, 4)
    except:
        return 0.0


def compute_all_alphas(df: pd.DataFrame) -> Dict[str, float]:
    """计算所有Alpha因子"""
    if df.empty or len(df) < 60:
        return {}
    
    prices = df['close']
    volume = df['volume']
    returns = df['close'].pct_change()
    
    alphas = {
        # 动量因子
        "alpha_001_momentum_5d": alpha_001(returns, 5),
        "alpha_002_momentum_20d": alpha_002(returns, 20),
        "alpha_003_momentum_60d": alpha_003(returns, 60),
        
        # 成交量因子
        "alpha_004_volume_momentum": alpha_004(volume, 5),
        "alpha_008_vwap_trend": alpha_008(volume, prices, 20),
        "alpha_017_volume_anomaly": alpha_017(volume, 20),
        
        # 价格因子
        "alpha_005_price_position": alpha_005(prices, 20),
        "alpha_009_ma_deviation": alpha_009(prices, 20),
        "alpha_013_price_breakout": alpha_013(prices, 20),
        "alpha_014_bb_position": alpha_014(prices, 20),
        "alpha_016_ma_cross": alpha_016(prices, 20),
        
        # 波动率因子
        "alpha_006_volatility": alpha_006(returns, 20),
        "alpha_007_price_acceleration": alpha_007(prices, 10),
        "alpha_018_annualized_vol": alpha_018(prices, 20),
        "alpha_019_drawdown": alpha_019(prices, 20),
        
        # 分布因子
        "alpha_010_return_skewness": alpha_010(returns, 20),
        "alpha_011_return_kurtosis": alpha_011(returns, 20),
        
        # 资金流
        "alpha_012_money_flow": alpha_012(prices, volume, 20),
        
        # 趋势
        "alpha_015_trend_strength": alpha_015(returns, 10),
        
        # 风险调整
        "alpha_020_sharpe_ratio": alpha_020(prices, returns, 20),
    }
    
    return alphas


def calculate_alpha_score(alphas: Dict[str, float]) -> float:
    """
    计算综合Alpha得分
    基于多因子加权计算整体信号强度
    """
    if not alphas:
        return 0.0
    
    weights = {
        # 动量因子权重
        "alpha_001_momentum_5d": 0.10,
        "alpha_002_momentum_20d": 0.10,
        "alpha_003_momentum_60d": 0.08,
        
        # 成交量因子权重
        "alpha_004_volume_momentum": 0.05,
        "alpha_008_vwap_trend": 0.05,
        "alpha_017_volume_anomaly": 0.02,
        
        # 价格因子权重
        "alpha_005_price_position": 0.08,
        "alpha_009_ma_deviation": 0.05,
        "alpha_013_price_breakout": 0.05,
        "alpha_014_bb_position": 0.05,
        "alpha_016_ma_cross": 0.08,
        
        # 波动率因子权重
        "alpha_006_volatility": 0.03,
        "alpha_019_drawdown": 0.03,
        
        # 风险调整
        "alpha_020_sharpe_ratio": 0.08,
        
        # 趋势
        "alpha_015_trend_strength": 0.05,
    }
    
    score = 0.0
    for name, weight in weights.items():
        if name in alphas:
            score += alphas[name] * weight
    
    # 归一化到 -1 到 1
    score = max(-1.0, min(1.0, score * 10))
    
    return round(score, 4)


def rank_alphas(alphas: Dict[str, float]) -> List[Dict[str, Any]]:
    """
    对Alpha因子进行排序和筛选
    返回最重要的因子
    """
    alpha_abs = {k: abs(v) for k, v in alphas.items() if abs(v) > 0.001}
    sorted_alphas = sorted(alpha_abs.items(), key=lambda x: x[1], reverse=True)
    
    return [
        {"name": name, "value": round(value, 6), "abs_value": round(abs(value), 6)}
        for name, value in sorted_alphas[:10]
    ]


# ============ API Endpoints ============

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "service": "alpha-miner"}


@app.get("/api/v1/alpha/factors")
async def get_alpha_factors(
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval")
):
    """获取所有Alpha因子"""
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    
    if df.empty:
        return JSONResponse(
            status_code=400,
            content={"error": "No data available"}
        )
    
    alphas = compute_all_alphas(df)
    alpha_score = calculate_alpha_score(alphas)
    top_alphas = rank_alphas(alphas)
    
    # 生成交易信号
    if alpha_score > 0.3:
        action = "BUY"
    elif alpha_score < -0.3:
        action = "SELL"
    else:
        action = "HOLD"
    
    return {
        "symbol": symbol,
        "interval": interval,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "alpha_score": alpha_score,
        "action": action,
        "top_factors": top_alphas,
        "all_alphas": alphas
    }


@app.get("/api/v1/alpha/score")
async def get_alpha_score(
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval")
):
    """获取综合Alpha得分"""
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    
    if df.empty:
        return JSONResponse(
            status_code=400,
            content={"error": "No data available"}
        )
    
    alphas = compute_all_alphas(df)
    alpha_score = calculate_alpha_score(alphas)
    
    return {
        "symbol": symbol,
        "interval": interval,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "alpha_score": alpha_score,
        "signal": "STRONG_BUY" if alpha_score > 0.5 else 
                  "BUY" if alpha_score > 0.2 else
                  "HOLD" if alpha_score > -0.2 else
                  "SELL" if alpha_score > -0.5 else "STRONG_SELL",
        "confidence": min(abs(alpha_score) * 2, 1.0)
    }


@app.get("/api/v1/alpha/factor/{factor_name}")
async def get_specific_alpha(
    factor_name: str,
    symbol: str = Query("BTCUSDT", description="Trading symbol"),
    interval: str = Query("1h", description="Time interval")
):
    """获取特定Alpha因子"""
    df = await fetch_ohlcv_data(symbol, interval, limit=200)
    
    if df.empty:
        return JSONResponse(
            status_code=400,
            content={"error": "No data available"}
        )
    
    prices = df['close']
    volume = df['volume']
    returns = df['close'].pct_change()
    
    alpha_functions = {
        "alpha_001": lambda: alpha_001(returns, 5),
        "alpha_002": lambda: alpha_002(returns, 20),
        "alpha_003": lambda: alpha_003(returns, 60),
        "alpha_004": lambda: alpha_004(volume, 5),
        "alpha_005": lambda: alpha_005(prices, 20),
        "alpha_006": lambda: alpha_006(returns, 20),
        "alpha_007": lambda: alpha_007(prices, 10),
        "alpha_008": lambda: alpha_008(volume, prices, 20),
        "alpha_009": lambda: alpha_009(prices, 20),
        "alpha_010": lambda: alpha_010(returns, 20),
        "alpha_011": lambda: alpha_011(returns, 20),
        "alpha_012": lambda: alpha_012(prices, volume, 20),
        "alpha_013": lambda: alpha_013(prices, 20),
        "alpha_014": lambda: alpha_014(prices, 20),
        "alpha_015": lambda: alpha_015(returns, 10),
        "alpha_016": lambda: alpha_016(prices, 20),
        "alpha_017": lambda: alpha_017(volume, 20),
        "alpha_018": lambda: alpha_018(prices, 20),
        "alpha_019": lambda: alpha_019(prices, 20),
        "alpha_020": lambda: alpha_020(prices, returns, 20),
    }
    
    # 匹配完整名称或简化名称
    func = alpha_functions.get(factor_name) or alpha_functions.get(f"alpha_{factor_name.split('_')[1]}")
    
    if not func:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown factor: {factor_name}"}
        )
    
    value = func()
    
    return {
        "factor_name": factor_name,
        "symbol": symbol,
        "interval": interval,
        "value": value,
        "interpretation": interpret_alpha(factor_name, value)
    }


def interpret_alpha(factor_name: str, value: float) -> str:
    """解释Alpha因子的含义"""
    interpretations = {
        "alpha_001_momentum_5d": "短期动量 - 正值表示近期上涨趋势强劲",
        "alpha_002_momentum_20d": "中期动量 - 正值表示中期上涨趋势",
        "alpha_003_momentum_60d": "长期动量 - 正值表示长期上涨趋势",
        "alpha_004_volume_momentum": "成交量动量 - >1 表示成交量增加",
        "alpha_005_price_position": "价格位置 - 接近1表示接近高点,接近0表示接近低点",
        "alpha_006_volatility": "波动率 - 高波动意味着高风险",
        "alpha_009_ma_deviation": "均线偏离度 - 正值表示价格高于均线",
        "alpha_014_bb_position": "布林带位置 - 接近1超买,接近0超卖",
        "alpha_016_ma_cross": "均线交叉 - 正值金叉,负值死叉",
        "alpha_019_drawdown": "回撤 - 负值表示从高点下跌",
        "alpha_020_sharpe_ratio": "夏普比率 - 正值表示风险调整收益好",
    }
    return interpretations.get(factor_name, "Alpha因子")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8004"))
    uvicorn.run(app, host="0.0.0.0", port=port)
