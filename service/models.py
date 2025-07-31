from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class DepthLevel:
    """Represents a single level (price and quantity) in the order book depth."""
    price: float
    quantity: int
    orders: int

@dataclass
class OrderDepth:
    """Holds the buy (bid) and sell (ask) sides of the order book depth."""
    buy: List[DepthLevel] = field(default_factory=list)
    sell: List[DepthLevel] = field(default_factory=list)

@dataclass
class TickData:
    """Represents a single market data update (tick) for an instrument."""
    timestamp: datetime
    instrument_token: int
    stock_name: str

    last_price: Optional[float] = None
    last_traded_quantity: Optional[int] = None
    average_traded_price: Optional[float] = None
    volume_traded: Optional[int] = None
    total_buy_quantity: Optional[int] = None
    total_sell_quantity: Optional[int] = None
    ohlc_open: Optional[float] = None
    ohlc_high: Optional[float] = None
    ohlc_low: Optional[float] = None
    ohlc_close: Optional[float] = None
    change: Optional[float] = None
    depth: Optional[OrderDepth] = None
