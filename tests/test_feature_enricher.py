import pytest
from datetime import datetime
from collections import deque
from core.feature_enricher import FeatureEnricher
from common.models import TickData, OrderDepth, DepthLevel


@pytest.fixture
def enricher():
    """Initializes a fresh enricher for each test."""
    return FeatureEnricher()


def test_trade_sign_at_ask(enricher):
    """Verifies a tick hitting the ask price is classified as a Buy (+1)."""
    base_time = datetime.now()
    depth = OrderDepth(
        timestamp=base_time, stock_name="TEST", instrument_token=123,
        buy=[DepthLevel(price=100.0, quantity=100, orders=5)],
        sell=[DepthLevel(price=100.5, quantity=100, orders=5)]
    )
    # Required fields: timestamp, instrument_token, stock_name
    tick = TickData(timestamp=base_time, stock_name="TEST", instrument_token=123,
                    last_price=100.5, depth=depth)

    enriched = enricher.enrich_tick(tick, deque())
    assert enriched.trade_sign == 1


def test_large_trade_detection(enricher):
    """Verifies large trade detection using a pre-loaded threshold."""
    base_time = datetime.now()
    enricher.load_thresholds({"TEST": 1000}, {123: "TEST"})

    # First tick to establish baseline volume
    t1 = TickData(timestamp=base_time, instrument_token=123, stock_name="TEST", volume_traded=10000)
    enricher.enrich_tick(t1, deque())

    # Second tick with +1500 volume (greater than 1000 threshold)
    t2 = TickData(timestamp=base_time, instrument_token=123, stock_name="TEST", volume_traded=11500)
    enriched = enricher.enrich_tick(t2, deque())

    assert enriched.tick_volume == 1500
    assert enriched.is_large_trade is True


def test_absorption_refill_detection(enricher):
    """Verifies that multiple refills at the same price trigger absorption flags."""
    base_time = datetime.now()
    # Setup baseline
    depth1 = OrderDepth(timestamp=base_time, stock_name="TEST", instrument_token=123,
                        sell=[DepthLevel(100.5, 10, 1)], buy=[DepthLevel(100.0, 10, 1)])
    t1 = TickData(timestamp=base_time, instrument_token=123, stock_name="TEST",
                  last_price=100.5, volume_traded=1000, depth=depth1)
    enricher.enrich_tick(t1, deque())

    # Simulate 3 consecutive aggressive buys that don't move the price (Sell Absorption)
    for i in range(3):
        t_refill = TickData(timestamp=base_time, instrument_token=123, stock_name="TEST",
                            last_price=100.5, volume_traded=1000 + (10 * (i + 1)), depth=depth1)
        enriched = enricher.enrich_tick(t_refill, deque())

    # confirmation threshold is 2 refills as defined in ICEBERG_CONFIRMATION_THRESHOLD
    assert enriched.is_sell_absorption is True