from datetime import datetime

from pydantic import BaseModel


class PriceTick(BaseModel):
    coin: str
    price_usd: float
    timestamp: datetime
