# core/es_logger.py

from elasticsearch import AsyncElasticsearch
from datetime import datetime
from common import config
from common.logger import log

class ESLogger:
    def __init__(self):
        self.client = AsyncElasticsearch(config.ES_HOST)

    async def log_event(self, stock_name, event_type, side, price, vwap, scores,
                        tick_timestamp, entry_price=None, stop_loss=None, reason=None):
        """Inserts a new immutable event document with trade levels for alerting."""
        doc = {
            "@timestamp": datetime.utcnow().isoformat(),  # Processing Time
            "tick_timestamp": tick_timestamp.isoformat(), # Market Event Time
            "stock_name": stock_name,
            "event_type": event_type,  # ENTRY, EXIT, WATCH, REVERSAL_WARN
            "side": side,
            "price": price,            # Current execution price
            "entry_price": entry_price, # Targeted or actual entry level
            "stop_loss": stop_loss,    # Targeted stop level
            "vwap": vwap,
            "indicators": {
                "obv_score": scores.get('price_vs_obv', 0),
                "clv_score": scores.get('price_vs_clv', 0),
                "structure_ratio": scores.get('structure_ratio', 0)
            },
            "exit_reason": reason
        }
        try:
            await self.client.index(index=config.ES_INDEX_SIGNALS, document=doc)
        except Exception as e:
            log.error(f"ES Logging Error: {e}")