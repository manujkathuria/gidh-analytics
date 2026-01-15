import pytest
from datetime import datetime, timedelta
from core.bar_aggregator import BarAggregator
from common.models import EnrichedTick


@pytest.fixture
def aggregator():
    """Creates a 1-minute bar aggregator for testing."""
    return BarAggregator("TEST", 123, timedelta(minutes=1))


def test_bar_ohlc_progression(aggregator):
    """Verifies that High/Low/Close update correctly across multiple ticks."""
    base_time = datetime(2024, 1, 1, 10, 0, 0)

    ticks = [
        EnrichedTick(timestamp=base_time, stock_name="TEST", instrument_token=123, last_price=100.0),
        EnrichedTick(timestamp=base_time + timedelta(seconds=1), stock_name="TEST", instrument_token=123,
                     last_price=105.0),
        EnrichedTick(timestamp=base_time + timedelta(seconds=2), stock_name="TEST", instrument_token=123,
                     last_price=95.0)
    ]

    for t in ticks:
        aggregator.add_tick(t)

    bar = aggregator.building_bar
    assert bar.open == 100.0
    assert bar.high == 105.0
    assert bar.low == 95.0
    assert bar.close == 95.0


def test_cvd_accumulation(aggregator):
    """Ensures that Buy/Sell delta sums correctly within a single bar."""
    base_time = datetime(2024, 1, 1, 10, 0, 0)

    # 100 Buy volume, then 40 Sell volume
    t1 = EnrichedTick(timestamp=base_time, stock_name="TEST", instrument_token=123,
                      last_price=100, tick_volume=100, trade_sign=1)
    t2 = EnrichedTick(timestamp=base_time + timedelta(seconds=5), stock_name="TEST",
                      instrument_token=123, last_price=100, tick_volume=40, trade_sign=-1)

    aggregator.add_tick(t1)
    aggregator.add_tick(t2)

    # CVD (bar_delta) = 100 - 40 = 60
    assert aggregator.building_bar.raw_scores['bar_delta'] == 60


def test_bar_finalization(aggregator):
    """Verifies that a tick in a new minute correctly closes the previous bar."""
    t1 = EnrichedTick(timestamp=datetime(2024, 1, 1, 10, 0, 30), stock_name="TEST",
                      instrument_token=123, last_price=100)
    # This tick is in the next minute, should finalize the 10:00 bar
    t2 = EnrichedTick(timestamp=datetime(2024, 1, 1, 10, 1, 0), stock_name="TEST",
                      instrument_token=123, last_price=101)

    aggregator.add_tick(t1)
    completed_bar = aggregator.add_tick(t2)

    assert completed_bar is not None
    assert completed_bar.timestamp == datetime(2024, 1, 1, 10, 0, 0)