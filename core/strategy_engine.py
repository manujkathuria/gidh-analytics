import asyncio
from datetime import datetime
from common.logger import log
import core.db_writer as db_writer


class StrategyEngine:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.target_interval = "10m"

        # --- CAPITAL & RISK MANAGEMENT ---
        self.TOTAL_CAPITAL = 100000.0  # Initial ₹1 Lakh
        self.RISK_PER_TRADE_PCT = 0.01  # Risk 1% (₹1,000) per trade
        self.MAX_LEVERAGE = 5.0  # Max 5x Intraday Leverage
        self.MAX_CONCURRENT_TRADES = 3  # Portfolio limit

        # --- STRATEGY PARAMETERS (Research Validated) ---
        self.THRESHOLD = -0.5  # High-Intensity Filter
        self.SL_PCT = 0.0035  # 0.35% Stop Loss
        self.BE_TRIGGER_PCT = 0.0012  # Move to BE at +0.12% profit
        self.TP1_PCT = 0.0020  # Scale out 50% at +0.20% profit
        self.TP2_PCT = 0.0045  # Final target at +0.45%
        self.SAFETY_TIMEOUT_BARS = 15  # Safety net (2.5 hours)

        # --- STATE MANAGEMENT ---
        self.active_trades = {}  # {stock_name: trade_details}
        self.last_signal_timestamps = {}

    async def run_logic(self, bar):
        """
        Main entry point called by the pipeline for every finalized bar.
        Handles both trade management and new signal detection.
        """
        stock = bar.stock_name

        # 1. Manage Active Positions
        if stock in self.active_trades:
            await self._manage_active_trade(bar)
            return

        # 2. Filtering
        if bar.interval != self.target_interval:
            return

        # 3. Portfolio Limit Check
        if len(self.active_trades) >= self.MAX_CONCURRENT_TRADES:
            return

        # 4. Extract Metrics
        scores = bar.raw_scores
        div = scores.get('divergence', {})
        div_obv = div.get('price_vs_obv', 0)
        div_clv = div.get('price_vs_clv', 0)
        structure = scores.get('structure', 'init')
        typical_price = (bar.high + bar.low + bar.close) / 3

        # 5. ENTRY CONDITION: Intensity + Trap + Structure Filter
        if div_obv < self.THRESHOLD and div_clv < self.THRESHOLD and structure != 'down':
            if bar.close > typical_price:
                # Prevent re-triggering on same bar
                last_ts = self.last_signal_timestamps.get(stock)
                if last_ts and bar.timestamp <= last_ts:
                    return

                await self._execute_short_entry(bar, div_obv, div_clv, structure)

    async def _execute_short_entry(self, bar, d_obv, d_clv, struct):
        entry_price = bar.close

        # POSITION SIZING: Risk ₹1,000 per trade based on SL distance
        cash_risk = self.TOTAL_CAPITAL * self.RISK_PER_TRADE_PCT
        risk_per_share = entry_price * self.SL_PCT
        quantity = int(cash_risk / risk_per_share) if risk_per_share > 0 else 0

        # Leverage Safety Check (Max 5x)
        max_allowed_exposure = self.TOTAL_CAPITAL * self.MAX_LEVERAGE
        if (quantity * entry_price) > max_allowed_exposure:
            quantity = int(max_allowed_exposure / entry_price)
            log.warning(f"⚠️ Leverage cap hit for {bar.stock_name}. Sizing down.")

        if quantity <= 0:
            return

        trade = {
            'entry_price': entry_price,
            'quantity': quantity,
            'remaining_qty': quantity,
            'realized_pnl_cash': 0.0,
            'stop_loss': entry_price * (1 + self.SL_PCT),
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

        log.info(f"🚀 [ENTRY] {bar.stock_name} SHORT {quantity} shares @ {entry_price}")

        # Log to Database
        await db_writer.insert_signal(self.db_pool, {
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
        })

    async def _manage_active_trade(self, bar):
        t = self.active_trades[bar.stock_name]
        price = bar.close
        t['bar_count'] += 1

        # A. SIGNAL-BASED EXIT (Phase Resolved)
        div = bar.raw_scores.get('divergence', {})
        if div.get('price_vs_obv', 0) >= 0 or div.get('price_vs_clv', 0) >= 0:
            await self._close_position(bar, "SIGNAL_INVALIDATED")
            return

        # B. BREAK-EVEN MANAGEMENT
        if not t['is_be_active'] and price <= t['be_trigger']:
            t['stop_loss'] = t['entry_price']
            t['is_be_active'] = True
            log.info(f"🛡️ [BE] {bar.stock_name} stop moved to Entry.")

        # C. SCALE OUT (TP1)
        if not t['is_tp1_hit'] and price <= t['tp1']:
            scale_qty = int(t['quantity'] * 0.5)
            profit = (t['entry_price'] - price) * scale_qty
            t['realized_pnl_cash'] += profit
            t['remaining_qty'] -= scale_qty
            t['is_tp1_hit'] = True
            t['stop_loss'] = t['entry_price']  # De-risk completely
            log.info(f"💰 [TP1] {bar.stock_name} Scaled 50%. Profit: ₹{round(profit, 2)}")

        # D. FINAL BRACKET & SAFETY EXITS
        if price >= t['stop_loss']:
            reason = "BE_EXIT" if t['is_be_active'] else "STOP_LOSS"
            await self._close_position(bar, reason)
        elif price <= t['tp2']:
            await self._close_position(bar, "TP2_FULL_MARKDOWN")
        elif t['bar_count'] >= self.SAFETY_TIMEOUT_BARS:
            await self._close_position(bar, "SAFETY_TIME_EXIT")

    async def _close_position(self, bar, reason):
        t = self.active_trades.pop(bar.stock_name)

        # Calculate PnL for the remaining quantity
        remaining_pnl = (t['entry_price'] - bar.close) * t['remaining_qty']
        total_pnl_cash = t['realized_pnl_cash'] + remaining_pnl
        pnl_pct = (total_pnl_cash / (t['entry_price'] * t['quantity'])) * 100

        # COMPOUNDING: Update capital for next trade sizing
        self.TOTAL_CAPITAL += total_pnl_cash

        # Log to DB
        await db_writer.update_signal_exit(self.db_pool, {
            'stock_name': bar.stock_name,
            'entry_time': t['timestamp'],
            'exit_timestamp': bar.timestamp,
            'exit_price': bar.close,
            'exit_reason': reason,
            'pnl_pct': pnl_pct,
            'pnl_cash': total_pnl_cash,
            'status': 'CLOSED'
        })

        log.info(f"🏁 [EXIT] {bar.stock_name} via {reason} | Net PnL: ₹{round(total_pnl_cash, 2)}")