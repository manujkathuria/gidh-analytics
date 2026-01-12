# core/es_logger.py

from elasticsearch import AsyncElasticsearch
from datetime import datetime
from common import config
from common.logger import log

class ESLogger:
    def __init__(self):
        # No password needed based on your xpack.security.enabled=false config
        self.client = AsyncElasticsearch(config.ES_HOST)

    async def log_event(self, stock_name, event_type, side, price, vwap, scores, reason=None):
        """Inserts a new immutable event document for Kibana timeline tracking."""
        doc = {
            "@timestamp": datetime.utcnow().isoformat(),
            "stock_name": stock_name,
            "event_type": event_type,  # WATCH, ENTRY, EXIT, REVERSAL_WARN
            "side": side,
            "price": price,
            "vwap": vwap,
            "dist_from_vwap_pct": round(((price - vwap) / vwap) * 100, 4) if vwap else 0,
            "indicators": {
                "obv_score": scores.get('price_vs_obv', 0),
                "clv_score": scores.get('price_vs_clv', 0)
            },
            "exit_reason": reason
        }
        try:
            await self.client.index(index=config.ES_INDEX_SIGNALS, document=doc)
        except Exception as e:
            log.error(f"ES Logging Error: {e}")

    async def close(self):
        await self.client.close()