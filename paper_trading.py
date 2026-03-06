"""
paper_trading.py
────────────────
Paper trading ledger that sits on top of the existing trade_ideas +
price_snapshots infrastructure.

Design principles:
  - No new tables required — reads from v_idea_outcomes (already exists)
  - One paper trade per idea (idea_id is the natural key)
  - Fixed notional per size_tier: HIGH=$10k, MEDIUM=$5k, LOW=$2k
  - P&L is direction-adjusted: LONG profits when price rises, SHORT when falls
  - Positions auto-close when age > time_horizon or stop-loss hit
  - Calibration report bins ideas by confidence decile → actual win rate

Usage:
  # From pipeline (after idea generated + entry price snapshotted):
  from paper_trading import PaperLedger
  ledger = PaperLedger()
  ledger.open_position(idea)          # call once per idea

  # Cron (runs alongside outcome_tracking price fetch):
  ledger.update_all_open_positions()  # mark-to-market, close expired
  ledger.print_summary()              # P&L report

  # Standalone:
  python paper_trading.py
"""

from __future__ import annotations

import os
import json
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── Optional DB import (graceful fallback for unit tests) ─────────────────────
try:
    from db.database import Database
    _DB_AVAILABLE = True
except ImportError:
    _DB_AVAILABLE = False

# ── Constants ─────────────────────────────────────────────────────────────────

# Virtual notional per size tier (USD)
NOTIONAL = {
    "HIGH":   10_000,
    "MEDIUM":  5_000,
    "LOW":     2_000,
}
DEFAULT_NOTIONAL = 2_000   # fallback if size_tier missing

# Stop-loss and take-profit thresholds (as a fraction of notional)
STOP_LOSS_PCT   = -0.08    # -8%  → auto-close
TAKE_PROFIT_PCT =  0.20    # +20% → auto-close

# Default max holding period if time_horizon not parseable
DEFAULT_MAX_DAYS = 30

# ── Time-horizon parser ───────────────────────────────────────────────────────

def _parse_horizon_days(horizon: str | None) -> int:
    """
    Convert LLM time_horizon string to integer days.
    Handles: '2–4 weeks', '1-2 months', '3 days', '10 days', etc.
    Returns DEFAULT_MAX_DAYS if unparseable.
    """
    if not horizon:
        return DEFAULT_MAX_DAYS
    import re
    h = horizon.lower()

    # Extract the UPPER bound of any range (e.g. "2-4" → 4)
    nums = re.findall(r'\d+', h)
    if not nums:
        return DEFAULT_MAX_DAYS
    n = int(nums[-1])   # take upper bound

    if 'month' in h:
        return n * 30
    if 'week' in h:
        return n * 7
    if 'day' in h:
        return n
    return DEFAULT_MAX_DAYS


# ═══════════════════════════════════════════════════════════════════════════════
# POSITION
# ═══════════════════════════════════════════════════════════════════════════════

class Position:
    """
    Represents a single paper trade, derived from a trade_idea row.

    Lifecycle:
      OPEN   → position is live, P&L updates on each price fetch
      CLOSED → position closed by stop-loss, take-profit, or expiry
    """

    __slots__ = (
        "idea_id", "ticker", "direction", "confidence", "size_tier",
        "time_horizon", "thesis", "generated_at",
        "entry_price", "notional", "max_days",
        "current_price", "last_updated",
        "status", "close_reason", "closed_at",
    )

    def __init__(
        self,
        idea_id:      str,
        ticker:       str,
        direction:    str,      # 'LONG' | 'SHORT'
        confidence:   int,
        size_tier:    str | None,
        time_horizon: str | None,
        thesis:       str | None,
        generated_at: datetime,
        entry_price:  float,
    ):
        self.idea_id      = idea_id
        self.ticker       = ticker
        self.direction    = direction
        self.confidence   = confidence
        self.size_tier    = size_tier or "LOW"
        self.time_horizon = time_horizon
        self.thesis       = thesis
        self.generated_at = generated_at
        self.entry_price  = entry_price
        self.notional     = NOTIONAL.get(self.size_tier, DEFAULT_NOTIONAL)
        self.max_days     = _parse_horizon_days(time_horizon)

        self.current_price: float | None = None
        self.last_updated:  datetime | None = None
        self.status:        str = "OPEN"
        self.close_reason:  str | None = None
        self.closed_at:     datetime | None = None

    # ── P&L calculations ──────────────────────────────────────────────────────

    @property
    def return_pct(self) -> float | None:
        """Direction-adjusted return %."""
        if self.current_price is None or self.entry_price == 0:
            return None
        raw = (self.current_price - self.entry_price) / self.entry_price
        return raw if self.direction == "LONG" else -raw

    @property
    def pnl_usd(self) -> float | None:
        """Dollar P&L = notional × return_pct."""
        r = self.return_pct
        return None if r is None else self.notional * r

    @property
    def age_days(self) -> int:
        ref = self.closed_at or datetime.now(timezone.utc)
        delta = ref - self.generated_at
        return max(0, delta.days)

    @property
    def is_winner(self) -> bool | None:
        r = self.return_pct
        return None if r is None else r > 0

    # ── State transitions ─────────────────────────────────────────────────────

    def update_price(self, price: float) -> None:
        self.current_price = price
        self.last_updated  = datetime.now(timezone.utc)

    def maybe_close(self) -> bool:
        """
        Check stop-loss, take-profit, and expiry.
        Returns True if position was just closed.
        """
        if self.status != "OPEN":
            return False

        r = self.return_pct
        if r is not None:
            if r <= STOP_LOSS_PCT:
                self._close("stop_loss")
                return True
            if r >= TAKE_PROFIT_PCT:
                self._close("take_profit")
                return True

        if self.age_days >= self.max_days:
            self._close("expired")
            return True

        return False

    def _close(self, reason: str) -> None:
        self.status      = "CLOSED"
        self.close_reason = reason
        self.closed_at   = datetime.now(timezone.utc)

    # ── Display ───────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        r = self.return_pct
        pnl = self.pnl_usd
        r_str   = f"{r*100:+.1f}%" if r is not None else "?"
        pnl_str = f"${pnl:+.0f}"  if pnl is not None else "?"
        return (
            f"<Position {self.ticker} {self.direction} "
            f"conf={self.confidence} {r_str} {pnl_str} [{self.status}]>"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# LEDGER
# ═══════════════════════════════════════════════════════════════════════════════

class PaperLedger:
    """
    Paper trading ledger.

    Reads trade data from Supabase (via Database), maintains in-memory
    position state, and persists close events back to trade_ideas.outcome_*.
    """

    def __init__(self, db: "Database | None" = None):
        self._db:        "Database | None" = db
        self._positions: dict[str, Position] = {}   # idea_id → Position

    # ── DB helper ─────────────────────────────────────────────────────────────

    def _get_db(self) -> "Database":
        if self._db:
            return self._db
        if not _DB_AVAILABLE:
            raise RuntimeError("Database module not available")
        return Database.get()

    # ── Open a new position ───────────────────────────────────────────────────

    def open_position(self, idea: dict) -> Position | None:
        """
        Call this right after a trade idea is generated and entry price snapshotted.

        `idea` should be the dict returned by the LLM pipeline, containing:
          idea_id, ticker, direction, confidence, size_tier,
          time_horizon, thesis, generated_at, price_snapshot (entry price)
        """
        idea_id = idea.get("idea_id")
        ticker  = idea.get("ticker")
        if not idea_id or not ticker:
            return None

        if idea_id in self._positions:
            return self._positions[idea_id]   # already open

        # Get entry price from price_snapshot dict or direct field
        entry_price: float | None = None
        ps = idea.get("price_snapshot") or {}
        if isinstance(ps, dict):
            entry_price = ps.get("price") or ps.get(ticker, {}).get("price")
        if entry_price is None:
            entry_price = idea.get("entry_price")
        if not entry_price:
            return None   # can't trade without price

        generated_at = idea.get("generated_at")
        if isinstance(generated_at, str):
            generated_at = datetime.fromisoformat(generated_at)
        if generated_at is None:
            generated_at = datetime.now(timezone.utc)

        pos = Position(
            idea_id      = idea_id,
            ticker       = ticker,
            direction    = idea.get("direction", "LONG"),
            confidence   = idea.get("confidence", 50),
            size_tier    = idea.get("size_tier"),
            time_horizon = idea.get("time_horizon"),
            thesis       = idea.get("thesis"),
            generated_at = generated_at,
            entry_price  = float(entry_price),
        )
        self._positions[idea_id] = pos
        return pos

    # ── Load open positions from DB ───────────────────────────────────────────

    def load_open_positions(self) -> int:
        """
        Reload all open positions from v_idea_outcomes.
        Call at startup or after restart to resume tracking.
        Returns number of positions loaded.
        """
        db = self._get_db()
        rows = db._execute("""
            SELECT
                idea_id, ticker, direction, confidence,
                size_tier, time_horizon, thesis, generated_at,
                entry_price, current_price, age_days,
                outcome_return_pct
            FROM v_idea_outcomes
            WHERE entry_price IS NOT NULL
        """, fetch=True)

        loaded = 0
        for row in (rows or []):
            idea_id = row["idea_id"]
            if idea_id in self._positions:
                continue

            ep = row.get("entry_price")
            if not ep:
                continue

            gen_at = row.get("generated_at") or datetime.now(timezone.utc)

            pos = Position(
                idea_id      = idea_id,
                ticker       = row["ticker"],
                direction    = row["direction"],
                confidence   = row.get("confidence") or 50,
                size_tier    = row.get("size_tier"),
                time_horizon = row.get("time_horizon"),
                thesis       = row.get("thesis"),
                generated_at = gen_at,
                entry_price  = float(ep),
            )
            cp = row.get("current_price")
            if cp:
                pos.update_price(float(cp))
            self._positions[idea_id] = pos
            loaded += 1

        return loaded

    # ── Mark-to-market update ─────────────────────────────────────────────────

    def update_position(self, idea_id: str, current_price: float) -> Position | None:
        """
        Update a position with the latest price and check close conditions.
        Call from the outcome_tracking cron after each yfinance fetch.
        """
        pos = self._positions.get(idea_id)
        if pos is None or pos.status != "OPEN":
            return pos

        pos.update_price(current_price)
        just_closed = pos.maybe_close()

        if just_closed:
            self._persist_close(pos)

        return pos

    def update_all_open_positions(self) -> dict:
        """
        Fetch latest prices for all open positions and mark-to-market.
        Designed to run alongside the outcome_tracking cron.

        Returns summary dict: {updated, closed_sl, closed_tp, closed_exp}
        """
        open_positions = [p for p in self._positions.values() if p.status == "OPEN"]
        if not open_positions:
            return {"updated": 0, "closed_sl": 0, "closed_tp": 0, "closed_exp": 0}

        try:
            import yfinance as yf
        except ImportError:
            return {"error": "yfinance not installed"}

        tickers = list({p.ticker for p in open_positions})
        try:
            import math
            # Fetch each ticker individually — batch download mishandles
            # single-letter tickers (X, T, F) and returns NaN for them.
            prices = {}
            for ticker in tickers:
                try:
                    tkr  = yf.Ticker(ticker)
                    hist = tkr.history(period="2d")   # 2d ensures we get at least 1 row
                    if hist.empty:
                        continue
                    price = float(hist["Close"].iloc[-1])
                    if not math.isnan(price) and price > 0:
                        prices[ticker] = price
                except Exception:
                    continue   # skip bad tickers, keep going
        except Exception:
            return {"error": "yfinance fetch failed"}

        stats = {"updated": 0, "closed_sl": 0, "closed_tp": 0, "closed_exp": 0}
        now_iso = datetime.now(timezone.utc).isoformat()

        for pos in open_positions:
            price = prices.get(pos.ticker)
            if price:
                pos.update_price(float(price))
                stats["updated"] += 1
                # Write to price_snapshots so v_idea_outcomes sees updated current_price
                self._persist_price_snapshot(pos, float(price), now_iso)

            just_closed = pos.maybe_close()
            if just_closed:
                self._persist_close(pos)
                key = {
                    "stop_loss":   "closed_sl",
                    "take_profit": "closed_tp",
                    "expired":     "closed_exp",
                }.get(pos.close_reason, "closed_exp")
                stats[key] += 1

        return stats

    # ── Persist close to DB ───────────────────────────────────────────────────

    def _persist_close(self, pos: Position) -> None:
        """Write close outcome back to trade_ideas.outcome_* columns."""
        try:
            db = self._get_db()
            r = pos.return_pct
            db.update_idea_outcome(
                idea_id     = pos.idea_id,
                return_pct  = round(r * 100, 4) if r is not None else None,
                days        = pos.age_days,
                notes       = f"paper_trade:{pos.close_reason}",
            )
        except Exception:
            pass   # non-critical — position is still tracked in memory

    def _persist_price_snapshot(self, pos: Position, price: float, now_iso: str) -> None:
        """
        Write an outcome_tracking snapshot to price_snapshots.
        This is what v_idea_outcomes reads for current_price — without
        this the dashboard always shows the entry price or last pipeline price.
        """
        try:
            db = self._get_db()
            db.insert_price_snapshot({
                "idea_id":      pos.idea_id,
                "ticker":       pos.ticker,
                "price":        round(price, 4),
                "fetched_at":   now_iso,
                "fetch_reason": "outcome_tracking",
            })
        except Exception:
            pass   # non-critical

    # ── Reports ───────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        """
        Compute P&L summary across all positions.

        Returns:
          open_count, closed_count,
          total_pnl_usd, win_rate,
          avg_winner_pct, avg_loser_pct,
          by_direction, by_size_tier,
          calibration (confidence decile → actual win rate)
        """
        all_pos   = list(self._positions.values())
        open_pos  = [p for p in all_pos if p.status == "OPEN"]
        closed    = [p for p in all_pos if p.status == "CLOSED" and p.return_pct is not None]

        def _stats(positions: list[Position]) -> dict:
            if not positions:
                return {"count": 0, "pnl_usd": 0.0, "win_rate": None,
                        "avg_win_pct": None, "avg_loss_pct": None}
            with_pnl = [p for p in positions if p.pnl_usd is not None]
            winners  = [p for p in with_pnl if p.is_winner]
            losers   = [p for p in with_pnl if not p.is_winner]
            total_pnl = sum(p.pnl_usd for p in with_pnl)
            win_rate  = len(winners) / len(with_pnl) if with_pnl else None
            avg_win   = (sum(p.return_pct for p in winners) / len(winners) * 100
                        if winners else None)
            avg_loss  = (sum(p.return_pct for p in losers) / len(losers) * 100
                        if losers else None)
            return {
                "count":        len(positions),
                "pnl_usd":      round(total_pnl, 2),
                "win_rate":     round(win_rate, 3) if win_rate is not None else None,
                "avg_win_pct":  round(avg_win, 2)  if avg_win  is not None else None,
                "avg_loss_pct": round(avg_loss, 2) if avg_loss is not None else None,
            }

        # Calibration: confidence decile → actual win rate
        calibration: dict[str, dict] = {}
        closed_with_outcome = [p for p in closed if p.return_pct is not None]
        for p in closed_with_outcome:
            # Bin into 10-point confidence buckets: "50-59", "60-69" etc.
            bucket = f"{(p.confidence // 10) * 10}-{(p.confidence // 10) * 10 + 9}"
            if bucket not in calibration:
                calibration[bucket] = {"total": 0, "wins": 0}
            calibration[bucket]["total"] += 1
            if p.is_winner:
                calibration[bucket]["wins"] += 1

        calibration_pct = {
            bucket: {
                "total":    v["total"],
                "wins":     v["wins"],
                "win_rate": round(v["wins"] / v["total"], 3) if v["total"] else None,
            }
            for bucket, v in sorted(calibration.items())
        }

        # By direction
        longs  = [p for p in closed if p.direction == "LONG"]
        shorts = [p for p in closed if p.direction == "SHORT"]

        # By size tier
        by_tier = {}
        for tier in ("HIGH", "MEDIUM", "LOW"):
            tier_pos = [p for p in closed if p.size_tier == tier]
            by_tier[tier] = _stats(tier_pos)

        return {
            "as_of":         datetime.now(timezone.utc).isoformat(),
            "open":          _stats(open_pos),
            "closed":        _stats(closed),
            "by_direction": {
                "LONG":  _stats(longs),
                "SHORT": _stats(shorts),
            },
            "by_size_tier":  by_tier,
            "calibration":   calibration_pct,
        }

    def print_summary(self) -> None:
        """Print a formatted P&L summary to stdout."""
        s = self.summary()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        print(f"\n{'═'*60}")
        print(f"  PAPER TRADING LEDGER  —  {now}")
        print(f"{'═'*60}")

        def _fmt_stats(label: str, st: dict, indent: int = 2) -> None:
            pad = " " * indent
            if st["count"] == 0:
                print(f"{pad}{label}: (none)")
                return
            wr  = f"{st['win_rate']*100:.0f}%" if st["win_rate"] is not None else "?"
            awp = f"{st['avg_win_pct']:+.1f}%"  if st["avg_win_pct"]  is not None else "?"
            alp = f"{st['avg_loss_pct']:+.1f}%" if st["avg_loss_pct"] is not None else "?"
            pnl_sign = "+" if st["pnl_usd"] >= 0 else ""
            print(f"{pad}{label}: n={st['count']}  "
                  f"P&L=${pnl_sign}{st['pnl_usd']:,.0f}  "
                  f"WR={wr}  avg_win={awp}  avg_loss={alp}")

        _fmt_stats("OPEN ", s["open"])
        _fmt_stats("CLOSED", s["closed"])

        print(f"\n  By direction:")
        _fmt_stats("LONG ", s["by_direction"]["LONG"],  indent=4)
        _fmt_stats("SHORT", s["by_direction"]["SHORT"], indent=4)

        print(f"\n  By size tier:")
        for tier in ("HIGH", "MEDIUM", "LOW"):
            _fmt_stats(tier, s["by_size_tier"][tier], indent=4)

        cal = s["calibration"]
        if cal:
            print(f"\n  Confidence calibration (closed trades):")
            print(f"    {'Conf':8s}  {'n':>4}  {'Wins':>5}  {'Win%':>6}  {'Ideal':>6}")
            print(f"    {'─'*40}")
            for bucket, v in sorted(cal.items()):
                actual  = f"{v['win_rate']*100:.0f}%" if v["win_rate"] is not None else "?"
                # Ideal = mid-point of bucket as probability
                mid = int(bucket.split("-")[0]) + 5
                ideal = f"{mid}%"
                stars = "★" if v["win_rate"] and abs(v["win_rate"] - mid / 100) < 0.1 else ""
                print(f"    {bucket:8s}  {v['total']:>4}  {v['wins']:>5}  {actual:>6}  {ideal:>6}  {stars}")

        print()

        # Open positions detail
        open_pos = [p for p in self._positions.values() if p.status == "OPEN"]
        if open_pos:
            print(f"  Open positions ({len(open_pos)}):")
            print(f"  {'Ticker':8s}  {'Dir':5s}  {'Conf':>4}  {'Age':>4}  "
                  f"{'Entry':>7}  {'Now':>7}  {'Return':>7}  {'P&L':>8}")
            print(f"  {'─'*65}")
            for p in sorted(open_pos, key=lambda x: -(x.return_pct or 0)):
                r   = p.return_pct
                pnl = p.pnl_usd
                r_s   = f"{r*100:+.1f}%"  if r   is not None else "?"
                pnl_s = f"${pnl:+.0f}"    if pnl is not None else "?"
                cp_s  = f"{p.current_price:.2f}" if p.current_price else "?"
                print(f"  {p.ticker:8s}  {p.direction:5s}  {p.confidence:>4}  "
                      f"{p.age_days:>3}d  "
                      f"{p.entry_price:>7.2f}  {cp_s:>7}  {r_s:>7}  {pnl_s:>8}")
            print()

        # Recently closed
        closed = sorted(
            [p for p in self._positions.values() if p.status == "CLOSED"],
            key=lambda x: x.closed_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )[:10]
        if closed:
            print(f"  Recently closed (last 10):")
            print(f"  {'Ticker':8s}  {'Dir':5s}  {'Return':>7}  {'P&L':>8}  "
                  f"{'Days':>5}  {'Reason':12s}")
            print(f"  {'─'*55}")
            for p in closed:
                r   = p.return_pct
                pnl = p.pnl_usd
                r_s   = f"{r*100:+.1f}%"  if r   is not None else "?"
                pnl_s = f"${pnl:+.0f}"    if pnl is not None else "?"
                print(f"  {p.ticker:8s}  {p.direction:5s}  {r_s:>7}  {pnl_s:>8}  "
                      f"{p.age_days:>5}  {p.close_reason or '?'}")
            print()

        print(f"{'═'*60}\n")

    # ── Open positions list ───────────────────────────────────────────────────

    def get_open_positions(self) -> list[Position]:
        return [p for p in self._positions.values() if p.status == "OPEN"]

    def get_position(self, idea_id: str) -> Position | None:
        return self._positions.get(idea_id)

    def __len__(self) -> int:
        return len(self._positions)


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE INTEGRATION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level singleton for pipeline use
_ledger: PaperLedger | None = None

def get_ledger() -> PaperLedger:
    """Return the module-level ledger singleton, loading open positions if needed."""
    global _ledger
    if _ledger is None:
        _ledger = PaperLedger()
        try:
            n = _ledger.load_open_positions()
        except Exception:
            n = 0
    return _ledger


def record_idea(idea: dict) -> Position | None:
    """
    Convenience function — call from pipeline Stage 10 (after idea saved + entry
    price snapshotted) to open a paper trade.

    Usage in pipeline.py:
        from paper_trading import record_idea
        position = record_idea(idea)
    """
    return get_ledger().open_position(idea)


def run_mtm_update() -> dict:
    """
    Convenience function for cron — fetch latest prices and update all open
    positions. Closes any that hit stop-loss, take-profit, or expiry.

    Usage in cron.py / outcome_tracking:
        from paper_trading import run_mtm_update
        result = run_mtm_update()
    """
    return get_ledger().update_all_open_positions()


# ═══════════════════════════════════════════════════════════════════════════════
# STANDALONE — print current ledger state from DB
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    ledger = PaperLedger()

    # ── Load from DB, fall back to mock if empty or unavailable ─────────────
    use_mock = False
    try:
        n = ledger.load_open_positions()
        if n == 0:
            print("DB available but no positions yet — showing mock data.")
            use_mock = True
        else:
            print(f"Loaded {n} position(s) from DB.")
    except Exception as e:
        print(f"DB not available ({e}) — showing mock data.\n")
        use_mock = True

    if use_mock:
        from datetime import timedelta
        mock_ideas = [
            {
                "idea_id": "demo_001", "ticker": "HD",   "direction": "LONG",
                "confidence": 72, "size_tier": "HIGH",  "time_horizon": "2-4 weeks",
                "thesis": "Home Depot supply chain cost tailwind from lower lumber prices",
                "generated_at": datetime.now(timezone.utc) - timedelta(days=8),
                "entry_price": 385.20,
            },
            {
                "idea_id": "demo_002", "ticker": "FDX",  "direction": "SHORT",
                "confidence": 65, "size_tier": "MEDIUM","time_horizon": "1-2 weeks",
                "thesis": "FedEx volume guide-down due to e-commerce slowdown",
                "generated_at": datetime.now(timezone.utc) - timedelta(days=5),
                "entry_price": 248.50,
            },
            {
                "idea_id": "demo_003", "ticker": "FIVE", "direction": "LONG",
                "confidence": 81, "size_tier": "HIGH",  "time_horizon": "3-6 weeks",
                "thesis": "Five Below discount retail benefits from consumer trade-down",
                "generated_at": datetime.now(timezone.utc) - timedelta(days=12),
                "entry_price": 95.40,
            },
            {
                "idea_id": "demo_004", "ticker": "X",    "direction": "SHORT",
                "confidence": 58, "size_tier": "LOW",   "time_horizon": "2-3 weeks",
                "thesis": "US Steel margin pressure from auto sector slowdown",
                "generated_at": datetime.now(timezone.utc) - timedelta(days=3),
                "entry_price": 38.75,
            },
        ]
        mock_prices = {"HD": 401.80, "FDX": 231.90, "FIVE": 88.20, "X": 41.50}
        for idea in mock_ideas:
            pos = ledger.open_position(idea)
            if pos:
                price = mock_prices.get(idea["ticker"])
                if price:
                    pos.update_price(price)
                    pos.maybe_close()

    # Run mark-to-market — try live prices, fall back to DB snapshots
    open_pos = ledger.get_open_positions()
    if open_pos:
        # Check how many already have a current_price from load_open_positions
        priced   = [p for p in open_pos if p.current_price is not None]
        unpriced = [p for p in open_pos if p.current_price is None]

        if unpriced:
            print(f"Fetching live prices for {len(unpriced)} position(s)...")
            try:
                import yfinance as yf, math
                for pos in unpriced:
                    try:
                        hist  = yf.Ticker(pos.ticker).history(period="2d")
                        if not hist.empty:
                            price = float(hist["Close"].iloc[-1])
                            if not math.isnan(price) and price > 0:
                                pos.update_price(price)
                                pos.maybe_close()
                                print(f"  {pos.ticker}: ${price:.2f}")
                    except Exception as ticker_err:
                        print(f"  {pos.ticker}: fetch failed ({ticker_err})")
            except ImportError:
                print("  yfinance not installed — showing DB prices only")
        else:
            print(f"Using DB prices for {len(priced)} position(s) (run cron to refresh).")

    ledger.print_summary()