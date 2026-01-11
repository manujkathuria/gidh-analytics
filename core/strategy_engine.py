import asyncio
from datetime import datetime
from common.logger import log
import core.db_writer as db_writer


class StrategyEngine:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.target_interval = "10m"

        # --- RISK & REVENUE PARAMETERS ---
        self.RISK_CASH_PER_TRADE = 5000  # Amount to risk in cash (e.g., ₹5000)

        # --- STRATEGY PARAMETERS (Validated by Research) ---
        self.THRESHOLD = -0.5  # High-Intensity Filter
        self.BE_TRIGGER_PCT = 0.0012  # +0.12% move triggers Break-even
        self.TP1_PCT = 0.0020  # 0.20% (Scale out 50% / De-risk)
        self.TP2_PCT = 0.0045  # 0.45% (Markdown Phase Target)
        self.SL_PCT = 0.0035  # 0.35% (Noise Buffer Stop)
        self.TIME_EXIT_BARS = 15  # Safety Timeout (2.5 hours)

        # --- STATE MANAGEMENT ---
        self.active_trades = {}  # {stock_name: trade_details}
        self.last_signal_timestamps = {}

    async def run_logic(self, bar):
        """
        Main entry point called by the pipeline for every finalized bar.
        Manages the lifecycle of institutional distribution trades.
        """
        stock = bar.stock_name

        # 1. Manage Active Trades (Manage the Phase)
        if stock in self.active_trades:
            await self._manage_active_trade(bar)
            return

        # 2. Filtering for Entry
        if bar.interval != self.target_interval:
            return

        # 3. Extract Divergence and Structure
        scores = bar.raw_scores
        div = scores.get('divergence', {})
        div_obv = div.get('price_vs_obv', 0)
        div_clv = div.get('price_vs_clv', 0)
        structure = scores.get('structure', 'init')

        # Typical Price used for the 'Trap' filter
        typical_price = (bar.high + bar.low + bar.close) / 3

        # 4. Entry Condition: High Intensity + Price Trap + Non-Collapsed Structure
        if div_obv < self.THRESHOLD and div_clv < self.THRESHOLD and structure != 'down':
            # Ensure we are shorting into strength (Price > Typical)
            if bar.close > typical_price:
                # Prevent re-triggering on the exact same bar
                last_ts = self.last_signal_timestamps.get(stock)
                if last_ts and bar.timestamp <= last_ts:
                    return

                await self._execute_short_entry(bar, div_obv, div_clv, structure)

    async def _execute_short_entry(self, bar, d_obv, d_clv, struct):
        entry_price = bar.close
        stop_price = entry_price * (1 + self.SL_PCT)

        # POSITION SIZING: Calculate quantity based on cash risk
        # Risk per share = Stop Price - Entry Price
        risk_per_share = stop_price - entry_price
        quantity = int(self.RISK_CASH_PER_TRADE / risk_per_share) if risk_per_share > 0 else 1

        trade = {
            'entry_price': entry_price,
            'quantity': quantity,
            'remaining_qty': quantity,
            'realized_pnl_cash': 0.0,
            'stop_loss': stop_price,
            'tp1': entry_price * (1 - self.TP1_PCT),
            'tp2': entry_price * (1 - self.TP2_PCT),
            'be_trigger': entry_price * (1 - self.BE_TRIGGER_PCT),
            'is_be_active': False,
            'is_tp1_hit': False,
            'bar_count': 0,
            'timestamp': bar.timestamp
        }

        self.active_trades[bar.stock_name] = trade
        self.last_signal_timestamps[bar.stock_name] = bar.timestamp

        log.info(f"🚀 [ENTRY] {bar.stock_name} SHORT {quantity} shares @ {entry_price} | Div: {round(d_obv, 2)}")

        # LOG TO DB: Open Record
        signal_data = {
            'timestamp': bar.timestamp,
            'stock_name': bar.stock_name,
            'interval': bar.interval,
            'side': 'SHORT',
            'entry_price': entry_price,
            'quantity': quantity,
            'div_obv': d_obv,
            'div_clv': d_clv,
            'structure': struct,
            'status': 'OPEN'
        }
        await db_writer.insert_signal(self.db_pool, signal_data)

    async def _manage_active_trade(self, bar):
        """Monitors price action to adjust stops or exit the distribution phase."""
        t = self.active_trades[bar.stock_name]
        price = bar.close
        t['bar_count'] += 1

        # 1. SIGNAL-BASED EXIT: Distribution Phase Resolved
        div = bar.raw_scores.get('divergence', {})
        if div.get('price_vs_obv', 0) >= 0 or div.get('price_vs_clv', 0) >= 0:
            await self._close_position(bar, "SIGNAL_INVALIDATED")
            return

        # 2. BREAK-EVEN MANAGEMENT
        if not t['is_be_active'] and price <= t['be_trigger']:
            t['stop_loss'] = t['entry_price']
            t['is_be_active'] = True
            log.info(f"🛡️ [BE] {bar.stock_name} stop moved to Entry.")

        # 3. TP1 SCALE-OUT (De-risking 50%)
        if not t['is_tp1_hit'] and price <= t['tp1']:
            scale_qty = int(t['quantity'] * 0.5)
            # Profit = (Entry - Exit) * Qty
            profit = (t['entry_price'] - price) * scale_qty
            t['realized_pnl_cash'] += profit
            t['remaining_qty'] -= scale_qty
            t['is_tp1_hit'] = True
            t['stop_loss'] = t['entry_price']  # Ensure BE
            log.info(f"💰 [TP1] {bar.stock_name} Scaled out 50%. Realized: {round(profit, 2)}")

        # 4. BRACKET & TIME EXITS
        if price >= t['stop_loss']:
            reason = "BE_EXIT" if t['is_be_active'] else "STOP_LOSS"
            await self._close_position(bar, reason)

        elif price <= t['tp2']:
            await self._close_position(bar, "TP2_FULL_MARKDOWN")

        elif t['bar_count'] >= self.TIME_EXIT_BARS:
            await self._close_position(bar, "SAFETY_TIME_EXIT")

    async def _close_position(self, bar, reason):
        """Finalizes the trade, logs the Revenue, and clears state."""
        t = self.active_trades.pop(bar.stock_name)

        # Calculate PnL for the remaining quantity
        remaining_pnl = (t['entry_price'] - bar.close) * t['remaining_qty']
        total_pnl_cash = t['realized_pnl_cash'] + remaining_pnl
        pnl_pct = (total_pnl_cash / (t['entry_price'] * t['quantity'])) * 100

        # LOG TO DB: Update Record
        exit_data = {
            'stock_name': bar.stock_name,
            'entry_time': t['timestamp'],
            'exit_timestamp': bar.timestamp,
            'exit_price': bar.close,
            'exit_reason': reason,
            'pnl_pct': pnl_pct,
            'pnl_cash': total_pnl_cash,
            'status': 'CLOSED'
        }
        await db_writer.update_signal_exit(self.db_pool, exit_data)

        log.info(f"🏁 [EXIT] {bar.stock_name} via {reason} | Total Revenue: {round(total_pnl_cash, 2)}")