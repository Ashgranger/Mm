"""
Lighter Perpetual Market Maker — v3
====================================

Strategy
--------
Quote bid/ask symmetrically around a fair-value mid using a dynamic
half-spread that adapts to realised volatility and fill rate:

    half_spread = max(MIN_HALF_SPREAD_BPS, k * vol_ema)

    bid = fair_mid - half_spread  (in price units)
    ask = fair_mid + half_spread

Both sides ALWAYS reprice together when mid moves >= REPRICE_BPS.
This prevents the "ask goes stale while bid chases down" failure mode
that appeared in the dry-run logs.

Fair mid = (best_bid + best_ask) / 2, with optional inventory skew.

Fill-rate feedback (auto-tune)
-------------------------------
- No fill for > TIGHTEN_AFTER_S  → shrink half_spread by TIGHTEN_STEP
- Fill rate > WIDEN_ABOVE_PER_MIN → grow half_spread by WIDEN_STEP
- Hard floor: MIN_HALF_SPREAD_BPS
- Hard ceiling: MAX_HALF_SPREAD_BPS

Volatility regimes
-------------------
CALM   (vol < VOL_ACTIVE_BPS)  : normal quoting
ACTIVE (vol >= VOL_ACTIVE_BPS) : half_spread floor raised to vol * K_ACTIVE
SPIKE  (vol >= VOL_SPIKE_BPS)  : cancel all, pause SPIKE_PAUSE_S seconds

Speed optimisations
--------------------
- Both sides placed in one batch HTTP call (sign locally, one round-trip)
- Both sides modified in one batch HTTP call on reprice
- cancel_all_orders single tx on spike/shutdown
- asyncio.Event wake — zero-sleep reaction to every OB tick
- No lock in WS hot path — Python GIL makes float assignment atomic
- Latency logged in µs for every order action

DRY RUN
--------
DRY_RUN=true in .env — full simulation, no orders sent.
"""

import asyncio
import logging
import os
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Tuple

try:
    import lighter
    from lighter import ApiClient, WsClient
    from lighter.configuration import Configuration
except ModuleNotFoundError:
    raise SystemExit("\n[ERROR] Run: pip install lighter-sdk\n")


# ─────────────────────────────────────────────────────────────
#  .ENV LOADER
# ─────────────────────────────────────────────────────────────

def _load_dotenv(path: str = ".env") -> None:
    p = Path(path)
    if not p.exists():
        return
    with open(p, encoding="utf-8") as f:
        for raw in f:
            line = raw.replace("\xa0", " ").replace("\u00a0", " ").strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            for sep in (" #", "\t#"):
                if sep in val:
                    val = val[: val.index(sep)].strip()
                    break
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            val = val.strip()
            if key and key not in os.environ:
                os.environ[key] = val


_load_dotenv(os.getenv("ENV_FILE", ".env"))


# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────

BASE_URL      = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
API_KEYS: Dict[int, str] = {}

MARKET_INDEX     = int(os.getenv("LIGHTER_MARKET_INDEX", "92"))
ORDER_SIZE_BASE  = float(os.getenv("ORDER_SIZE_BASE",      "0.01"))
MAX_INVENTORY    = float(os.getenv("MAX_INVENTORY",        "0.1"))
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD",     "500.0"))
SKEW_FACTOR      = float(os.getenv("SKEW_FACTOR",          "0.3"))
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")
DRY_RUN          = os.getenv("DRY_RUN", "false").strip().lower() in ("true", "1", "yes")

# ── Spread / quoting ──────────────────────────────────────────
# Both bid and ask are placed at:  fair_mid ± half_spread_price
MIN_HALF_SPREAD_BPS = float(os.getenv("MIN_HALF_SPREAD_BPS", "5.0"))   # floor
MAX_HALF_SPREAD_BPS = float(os.getenv("MAX_HALF_SPREAD_BPS", "30.0"))  # ceiling
K_VOL               = float(os.getenv("K_VOL",               "1.5"))   # half_spread = max(floor, K_VOL * vol)
MIN_MARKET_SPREAD   = float(os.getenv("MIN_MARKET_SPREAD",   "0.5"))   # skip if book spread < this bps
REPRICE_BPS         = float(os.getenv("REPRICE_BPS",         "0.5"))   # reprice both sides when mid moves this much

# ── Fill-rate auto-tune ───────────────────────────────────────
TIGHTEN_AFTER_S     = float(os.getenv("TIGHTEN_AFTER_S",   "90.0"))   # no fill for this long → tighten
TIGHTEN_STEP_BPS    = float(os.getenv("TIGHTEN_STEP_BPS",  "0.5"))    # step size for tightening
WIDEN_ABOVE_FPM     = float(os.getenv("WIDEN_ABOVE_FPM",   "2.0"))    # fills/min threshold → widen
WIDEN_STEP_BPS      = float(os.getenv("WIDEN_STEP_BPS",    "1.0"))    # step size for widening

# ── Volatility ────────────────────────────────────────────────
VOL_WINDOW      = int(os.getenv("VOL_WINDOW",     "30"))    # ticks for EMA
VOL_ACTIVE_BPS  = float(os.getenv("VOL_ACTIVE_BPS", "3.0")) # ACTIVE regime threshold
VOL_SPIKE_BPS   = float(os.getenv("VOL_SPIKE_BPS", "15.0")) # SPIKE (cancel+pause) threshold
K_ACTIVE        = float(os.getenv("K_ACTIVE",       "2.0"))  # extra spread multiplier in ACTIVE
SPIKE_PAUSE_S   = float(os.getenv("SPIKE_PAUSE_S",  "2.0"))  # seconds to pause after spike

# ── Market precision ──────────────────────────────────────────
PRICE_SCALE = int(os.getenv("PRICE_SCALE", "100"))      # price_int = round(price_float * PRICE_SCALE)
SIZE_SCALE  = int(os.getenv("SIZE_SCALE",  "10000000")) # size_int  = round(size_float  * SIZE_SCALE)


# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mm")


# ─────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────

class Regime(Enum):
    CALM   = "calm"
    ACTIVE = "active"
    SPIKE  = "spike"


@dataclass
class Order:
    coi: int
    soi: Optional[int]  = None   # server order index
    price: float        = 0.0
    size: float         = 0.0
    is_ask: bool        = False
    status: str         = "pending"
    filled: float       = 0.0
    remaining: float    = 0.0
    sim: bool           = False


@dataclass
class Stats:
    placed: int   = 0
    batches: int  = 0
    fills: int    = 0
    bid_fills: int = 0
    ask_fills: int = 0
    partials: int = 0
    modifies: int = 0
    cancels: int  = 0
    volume: float = 0.0
    pnl: float    = 0.0
    _last_bid_px: float = 0.0
    t0: float = field(default_factory=time.time)

    def record_fill(self, is_ask: bool, px: float, qty: float):
        self.fills += 1
        self.volume += qty
        if is_ask:
            self.ask_fills += 1
            if self._last_bid_px:
                self.pnl += (px - self._last_bid_px) * qty
                self._last_bid_px = 0.0
        else:
            self.bid_fills += 1
            self._last_bid_px = px

    def fills_per_min(self) -> float:
        elapsed = time.time() - self.t0
        return self.fills / (elapsed / 60) if elapsed > 0 else 0.0

    def line(self) -> str:
        e = time.time() - self.t0
        return (
            f"up={e:.0f}s  placed={self.placed}(batch={self.batches})  "
            f"fills={self.fills}(B={self.bid_fills} A={self.ask_fills})  "
            f"partials={self.partials}  mod={self.modifies}  cancel={self.cancels}  "
            f"vol={self.volume:.6f}  pnl≈{self.pnl:.4f}"
        )


# ─────────────────────────────────────────────────────────────
#  ROLLING VOLATILITY  (EMA of |Δmid| bps per tick)
# ─────────────────────────────────────────────────────────────

class VolTracker:
    def __init__(self, window: int = 30):
        self._alpha = 2.0 / (window + 1)
        self._ema: Optional[float] = None
        self._prev: Optional[float] = None

    def update(self, mid: float) -> float:
        if self._prev is not None and self._prev > 0:
            ret = abs(mid - self._prev) / self._prev * 10_000
            self._ema = ret if self._ema is None else self._ema + self._alpha * (ret - self._ema)
        self._prev = mid
        return self._ema or 0.0

    @property
    def value(self) -> float:
        return self._ema or 0.0


# ─────────────────────────────────────────────────────────────
#  DRY-RUN SIMULATED EXCHANGE
# ─────────────────────────────────────────────────────────────

class SimExchange:
    def __init__(self):
        self._orders: Dict[int, Order] = {}
        self._cbs = []

    def on_fill(self, fn):
        self._cbs.append(fn)

    async def _fire(self, o: Order, qty: float):
        o.filled    += qty
        o.remaining  = max(0.0, o.size - o.filled)
        o.status     = "filled" if o.remaining < 1e-9 else "partial"
        upd = {
            "client_order_index":    o.coi,
            "status":                o.status,
            "filled_base_amount":    o.filled,
            "remaining_base_amount": o.remaining,
            "price":                 str(o.price),
        }
        for cb in self._cbs:
            await cb(upd)

    async def _check(self, o: Order, bb: float, ba: float):
        if o.status != "open":
            return
        if o.is_ask and bb > 0 and o.price <= bb:
            await self._fire(o, o.remaining)
        elif not o.is_ask and ba > 0 and o.price >= ba:
            await self._fire(o, o.remaining)

    async def place(self, coi: int, price: float, size: float,
                    is_ask: bool, bb: float, ba: float) -> Order:
        o = Order(coi=coi, soi=coi, price=price, size=size, is_ask=is_ask,
                  status="open", remaining=size, sim=True)
        self._orders[coi] = o
        await self._check(o, bb, ba)
        return o

    async def modify(self, o: Order, price: float, size: float,
                     bb: float, ba: float) -> bool:
        if o.status not in ("open", "partial"):
            return False
        o.price = price
        o.size  = size
        o.remaining = max(0.0, size - o.filled)
        if o.remaining > 0:
            o.status = "open"
        await self._check(o, bb, ba)
        return True

    async def cancel(self, o: Order) -> bool:
        if o.status in ("filled", "canceled"):
            return False
        o.status = "canceled"
        return True


# ─────────────────────────────────────────────────────────────
#  MARKET MAKER
# ─────────────────────────────────────────────────────────────

class MarketMaker:

    def __init__(self, client, api_client, dry_run: bool = False):
        self.client     = client
        self.api_client = api_client
        self.dry_run    = dry_run

        self.acct_api = lighter.AccountApi(api_client) if not dry_run else None

        self._sim = SimExchange()
        self._sim.on_fill(self._on_order_update)

        # ── Live market (written by WS thread — GIL makes float writes atomic)
        self.best_bid:  Optional[float] = None
        self.best_ask:  Optional[float] = None
        self.mid:       Optional[float] = None

        # ── Orders
        self.bid_ord: Optional[Order] = None
        self.ask_ord: Optional[Order] = None
        self._olock = asyncio.Lock()

        # ── COI counter
        self._coi = int(time.time() * 1_000) % (2**32)

        # ── Inventory
        self.pos:    float = 0.0
        self.avg_px: float = 0.0

        # ── Volatility + regime
        self._vol         = VolTracker(VOL_WINDOW)
        self._regime      = Regime.CALM
        self._spike_until = 0.0

        # ── Dynamic half-spread (bps) — starts at floor, auto-tunes
        self._half_spread_bps: float = MIN_HALF_SPREAD_BPS

        # ── Fill-rate tracking
        self._last_fill_time: float = time.time()
        self._last_tune_time: float = time.time()

        # ── Reprice tracking
        self._last_quoted_mid: Optional[float] = None

        # ── Stats + control
        self.stats    = Stats()
        self._wake    = asyncio.Event()
        self._running = True

        if dry_run:
            log.warning(
                "\n╔══════════════════════════════════════════════════╗"
                "\n║  🧪  DRY RUN — NO ORDERS SENT TO EXCHANGE        ║"
                "\n╚══════════════════════════════════════════════════╝"
            )

    # ── helpers ──────────────────────────────────────────────────

    def _ncoi(self) -> int:
        self._coi = (self._coi + 1) & 0x7FFF_FFFF_FFFF
        return self._coi

    def _pi(self, price: float) -> int:  # price → int
        return round(price * PRICE_SCALE)

    def _si(self, size: float) -> int:   # size  → int
        return round(size * SIZE_SCALE)

    def _bps(self, v) -> str:
        return f"{v:.2f}" if v is not None else "n/a"

    def _spread_bps(self) -> Optional[float]:
        if self.best_bid and self.best_ask and self.best_bid > 0:
            return (self.best_ask - self.best_bid) / self.best_bid * 10_000
        return None

    def _skew(self) -> float:
        """Inventory skew in bps: positive = long, want to sell."""
        if MAX_INVENTORY == 0:
            return 0.0
        return max(-1.0, min(1.0, self.pos / MAX_INVENTORY)) * SKEW_FACTOR

    def _order_size(self, is_ask: bool) -> float:
        ratio = self.pos / MAX_INVENTORY if MAX_INVENTORY else 0
        scale = (1 + ratio * 0.5) if is_ask else (1 - ratio * 0.5)
        return max(0.0, ORDER_SIZE_BASE * max(0.2, min(2.0, scale)))

    def _inventory_ok(self, is_ask: bool) -> bool:
        if abs(self.pos) >= MAX_INVENTORY:
            if not is_ask and self.pos >= MAX_INVENTORY:  return False
            if is_ask     and self.pos <= -MAX_INVENTORY: return False
        if self.mid and abs(self.pos) * self.mid >= MAX_POSITION_USD:
            if is_ask and self.pos > 0:     return True   # selling reduces long
            if not is_ask and self.pos < 0: return True   # buying reduces short
            return False
        return True

    # ── Dynamic half-spread ──────────────────────────────────────

    def _compute_half_spread_bps(self, vol: float) -> float:
        """
        half_spread = max(floor, K_VOL * vol)
        In ACTIVE regime, floor raised to vol * K_ACTIVE.
        Clipped to [MIN, MAX].
        """
        vol_spread = K_VOL * vol
        if self._regime == Regime.ACTIVE:
            floor = max(MIN_HALF_SPREAD_BPS, vol * K_ACTIVE)
        else:
            floor = MIN_HALF_SPREAD_BPS
        return min(MAX_HALF_SPREAD_BPS, max(floor, vol_spread))

    def _autotune_spread(self):
        """
        Every 30s check fill rate and gently adjust half_spread.
        - No fill for TIGHTEN_AFTER_S  → tighten by TIGHTEN_STEP_BPS
        - Fill rate > WIDEN_ABOVE_FPM  → widen by WIDEN_STEP_BPS
        """
        now = time.time()
        if now - self._last_tune_time < 30:
            return
        self._last_tune_time = now

        time_since_fill = now - self._last_fill_time
        fpm = self.stats.fills_per_min()

        if time_since_fill > TIGHTEN_AFTER_S:
            new = max(MIN_HALF_SPREAD_BPS, self._half_spread_bps - TIGHTEN_STEP_BPS)
            if new != self._half_spread_bps:
                log.info(
                    f"📉 AUTOTUNE tighten {self._half_spread_bps:.2f}→{new:.2f}bps "
                    f"(no fill for {time_since_fill:.0f}s)"
                )
                self._half_spread_bps = new

        elif fpm > WIDEN_ABOVE_FPM:
            new = min(MAX_HALF_SPREAD_BPS, self._half_spread_bps + WIDEN_STEP_BPS)
            if new != self._half_spread_bps:
                log.info(
                    f"📈 AUTOTUNE widen {self._half_spread_bps:.2f}→{new:.2f}bps "
                    f"(fill rate={fpm:.2f}/min)"
                )
                self._half_spread_bps = new

    def _target_quotes(self) -> Optional[Tuple[float, float]]:
        """
        Compute (bid_price, ask_price) symmetrically around fair mid.

        Both sides ALWAYS computed from the same mid → they always move
        in sync. This eliminates the "stale ask" problem from the logs.

        bid = fair_mid - half_spread_price
        ask = fair_mid + half_spread_price
        half_spread_price = fair_mid * half_spread_bps / 10_000
        """
        if self.mid is None or self.best_bid is None or self.best_ask is None:
            return None

        book_spread = self._spread_bps()
        if book_spread is None or book_spread < MIN_MARKET_SPREAD:
            return None

        vol        = self._vol.value
        half_bps   = self._compute_half_spread_bps(vol)
        # Keep current autotune value if it gives a wider spread than vol-computed
        half_bps   = max(half_bps, self._half_spread_bps)
        half_price = self.mid * half_bps / 10_000

        # Inventory skew (bps → price)
        skew_bps   = self._skew()
        skew_price = self.mid * skew_bps / 10_000

        bid_px = self.mid - half_price - skew_price
        ask_px = self.mid + half_price - skew_price

        tick   = 1.0 / PRICE_SCALE
        bid_px = round(bid_px / tick) * tick
        ask_px = round(ask_px / tick) * tick

        # Sanity: must not cross book and must have a gap
        if ask_px <= bid_px:
            return None

        return bid_px, ask_px

    def _should_reprice(self) -> bool:
        """True if mid moved >= REPRICE_BPS since last quote."""
        if self._last_quoted_mid is None or self.mid is None:
            return True
        move = abs(self.mid - self._last_quoted_mid) / self._last_quoted_mid * 10_000
        return move >= REPRICE_BPS

    # ── WebSocket callbacks ───────────────────────────────────────

    def on_ob_update(self, market_id, ob: dict):
        try:
            if int(market_id) != MARKET_INDEX:
                return
            asks = [a for a in ob.get("asks", []) if float(a.get("size", 0)) > 0]
            bids = [b for b in ob.get("bids", []) if float(b.get("size", 0)) > 0]
            if asks and bids:
                self.best_ask = float(asks[0]["price"])
                self.best_bid = float(bids[0]["price"])
                self.mid      = (self.best_bid + self.best_ask) / 2
                self._wake.set()
        except Exception as e:
            log.debug(f"OB: {e}")

    def on_account_update(self, account_id, account: dict):
        if self.dry_run:
            return
        for o in account.get("orders", []):
            asyncio.get_event_loop().call_soon_threadsafe(
                lambda order=o: asyncio.ensure_future(self._on_order_update(order))
            )

    # ── Fill / order update handler ───────────────────────────────

    async def _on_order_update(self, upd: dict):
        coi       = str(upd.get("client_order_index") or upd.get("client_order_id") or "")
        status    = upd.get("status", "")
        filled    = float(upd.get("filled_base_amount",    0) or 0)
        remaining = float(upd.get("remaining_base_amount", 0) or 0)
        soi       = upd.get("order_index") or upd.get("server_order_index")

        async with self._olock:
            for side, rec in [("bid", self.bid_ord), ("ask", self.ask_ord)]:
                if rec is None or str(rec.coi) != coi:
                    continue

                if soi and rec.soi is None:
                    rec.soi = int(soi)

                delta = filled - rec.filled
                if delta > 1e-10:
                    rec.filled    = filled
                    rec.remaining = remaining
                    self.stats.record_fill(rec.is_ask, rec.price, delta)
                    self._last_fill_time = time.time()

                    if rec.is_ask:
                        self.pos -= delta
                    else:
                        self.pos += delta

                    tag = "[DRY] " if self.dry_run else ""
                    log.info(
                        f"{tag}✅ FILL {'ASK' if rec.is_ask else 'BID'} "
                        f"+{delta:.6f}@{rec.price:.4f}  "
                        f"pos={self.pos:+.6f}  rem={remaining:.6f}  "
                        f"half_spread={self._half_spread_bps:.2f}bps"
                    )

                if status == "filled":
                    rec.status = "filled"
                    log.info(
                        f"{'[DRY] ' if self.dry_run else ''}🟢 FULL "
                        f"{'ASK' if rec.is_ask else 'BID'} coi={coi}"
                    )
                    asyncio.ensure_future(self._one_sided_fill(side, rec))

                elif status.startswith("canceled"):
                    rec.status = "canceled"
                    self.stats.cancels += 1

                elif remaining > 0 and remaining < rec.size and rec.status != "partial":
                    rec.status = "partial"
                    self.stats.partials += 1
                    log.info(
                        f"{'[DRY] ' if self.dry_run else ''}⚡ PARTIAL "
                        f"{'ASK' if rec.is_ask else 'BID'} "
                        f"{filled:.6f}/{rec.size:.6f}@{rec.price:.4f}"
                    )

    async def _one_sided_fill(self, filled_side: str, filled_rec: Order):
        """One leg fully filled → immediately reprice other leg at current quotes."""
        await asyncio.sleep(0.0)  # yield to release lock
        async with self._olock:
            other_is_ask = (filled_side == "bid")
            other = self.ask_ord if other_is_ask else self.bid_ord
            if other is None or other.status in ("filled", "canceled"):
                return
            quotes = self._target_quotes()
            if quotes is None:
                await self._cancel_one(other)
                return
            bid_px, ask_px = quotes
            new_px   = ask_px if other_is_ask else bid_px
            new_size = other.remaining if other.filled > 0 else self._order_size(other_is_ask)
            ok = await self._modify_one(other, new_px, new_size)
            if not ok:
                await self._cancel_one(other)
                rec = await self._place_one(other_is_ask, new_px, new_size)
                if rec:
                    if other_is_ask: self.ask_ord = rec
                    else:            self.bid_ord = rec

    # ── Order actions ─────────────────────────────────────────────

    async def _place_both(self, bid_px: float, ask_px: float
                          ) -> Tuple[Optional[Order], Optional[Order]]:
        """Place bid + ask in a single batch HTTP call."""
        bid_coi  = self._ncoi()
        ask_coi  = self._ncoi()
        bid_size = self._order_size(False)
        ask_size = self._order_size(True)
        tag      = "[DRY] " if self.dry_run else ""

        if self.dry_run:
            bb, ba = self.best_bid or 0.0, self.best_ask or 0.0
            b = await self._sim.place(bid_coi, bid_px, bid_size, False, bb, ba)
            a = await self._sim.place(ask_coi, ask_px, ask_size, True,  bb, ba)
            self.stats.placed  += 2
            self.stats.batches += 1
            log.info(
                f"{tag}📤 BATCH  BID {bid_px:.4f}×{bid_size:.6f}  "
                f"ASK {ask_px:.4f}×{ask_size:.6f}  "
                f"half={self._half_spread_bps:.2f}bps  [SIM]"
            )
            return b, a

        try:
            t0 = time.perf_counter_ns()
            bt, b_info, _, b_err = self.client.sign_create_order(
                MARKET_INDEX, bid_coi, self._si(bid_size), self._pi(bid_px),
                False, self.client.ORDER_TYPE_LIMIT,
                self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            )
            if b_err:
                log.warning(f"sign bid: {b_err}")
                return None, None
            at, a_info, _, a_err = self.client.sign_create_order(
                MARKET_INDEX, ask_coi, self._si(ask_size), self._pi(ask_px),
                True, self.client.ORDER_TYPE_LIMIT,
                self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            )
            if a_err:
                log.warning(f"sign ask: {a_err}")
                return None, None

            await self.client.send_tx_batch([bt, at], [b_info, a_info])
            us = (time.perf_counter_ns() - t0) // 1_000

            b = Order(coi=bid_coi, price=bid_px, size=bid_size, is_ask=False,
                      status="open", remaining=bid_size)
            a = Order(coi=ask_coi, price=ask_px, size=ask_size, is_ask=True,
                      status="open", remaining=ask_size)
            self.stats.placed  += 2
            self.stats.batches += 1
            log.info(
                f"📤 BATCH  BID {bid_px:.4f}×{bid_size:.6f}  "
                f"ASK {ask_px:.4f}×{ask_size:.6f}  "
                f"half={self._half_spread_bps:.2f}bps  {us}µs"
            )
            return b, a
        except Exception as e:
            log.error(f"place_both: {e}")
            return None, None

    async def _modify_both(self, bid_rec: Order, ask_rec: Order,
                           bid_px: float, ask_px: float) -> bool:
        """Modify bid + ask in a single batch HTTP call."""
        bid_size = bid_rec.remaining if bid_rec.filled > 0 else self._order_size(False)
        ask_size = ask_rec.remaining if ask_rec.filled > 0 else self._order_size(True)
        old_bid  = bid_rec.price
        old_ask  = ask_rec.price
        tag      = "[DRY] " if self.dry_run else ""

        if self.dry_run:
            bb, ba = self.best_bid or 0.0, self.best_ask or 0.0
            b_ok = await self._sim.modify(bid_rec, bid_px, bid_size, bb, ba)
            a_ok = await self._sim.modify(ask_rec, ask_px, ask_size, bb, ba)
            if b_ok or a_ok:
                self.stats.modifies += (1 if b_ok else 0) + (1 if a_ok else 0)
                self.stats.batches  += 1
                log.info(
                    f"{tag}✏️  BATCH MOD  "
                    f"BID {old_bid:.4f}→{bid_px:.4f}  "
                    f"ASK {old_ask:.4f}→{ask_px:.4f}  [SIM]"
                )
            return b_ok and a_ok

        if bid_rec.soi is None or ask_rec.soi is None:
            return False
        try:
            t0 = time.perf_counter_ns()
            bt, b_info, _, b_err = self.client.sign_modify_order(
                MARKET_INDEX, bid_rec.soi, self._si(bid_size), self._pi(bid_px)
            )
            if b_err: return False
            at, a_info, _, a_err = self.client.sign_modify_order(
                MARKET_INDEX, ask_rec.soi, self._si(ask_size), self._pi(ask_px)
            )
            if a_err: return False

            await self.client.send_tx_batch([bt, at], [b_info, a_info])
            us = (time.perf_counter_ns() - t0) // 1_000

            # Update records AFTER sending (so log shows old→new correctly)
            bid_rec.price = bid_px; bid_rec.size = bid_size
            bid_rec.remaining = max(0.0, bid_size - bid_rec.filled)
            ask_rec.price = ask_px; ask_rec.size = ask_size
            ask_rec.remaining = max(0.0, ask_size - ask_rec.filled)
            self.stats.modifies += 2
            self.stats.batches  += 1
            log.info(
                f"✏️  BATCH MOD  "
                f"BID {old_bid:.4f}→{bid_px:.4f}  "
                f"ASK {old_ask:.4f}→{ask_px:.4f}  {us}µs"
            )
            return True
        except Exception as e:
            log.error(f"modify_both: {e}")
            return False

    async def _place_one(self, is_ask: bool, price: float, size: float) -> Optional[Order]:
        coi  = self._ncoi()
        side = "ASK" if is_ask else "BID"
        tag  = "[DRY] " if self.dry_run else ""

        if self.dry_run:
            rec = await self._sim.place(coi, price, size, is_ask,
                                        self.best_bid or 0.0, self.best_ask or 0.0)
            self.stats.placed += 1
            log.info(f"{tag}📤 {side} {price:.4f}×{size:.6f}  [SIM]")
            return rec

        try:
            t0 = time.perf_counter_ns()
            tx, resp, err = await self.client.create_order(
                MARKET_INDEX, coi, self._si(size), self._pi(price), is_ask,
                self.client.ORDER_TYPE_LIMIT,
                self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            )
            us = (time.perf_counter_ns() - t0) // 1_000
            if err:
                log.warning(f"place {side}: {err}")
                return None
            rec = Order(coi=coi, price=price, size=size, is_ask=is_ask,
                        status="open", remaining=size)
            self.stats.placed += 1
            log.info(f"📤 {side} {price:.4f}×{size:.6f}  {us}µs")
            return rec
        except Exception as e:
            log.error(f"place {side}: {e}")
            return None

    async def _modify_one(self, rec: Order, price: float, size: float) -> bool:
        if rec.status not in ("open", "partial"):
            return False
        side    = "ASK" if rec.is_ask else "BID"
        tag     = "[DRY] " if self.dry_run else ""
        size    = max(size, rec.remaining)
        old_px  = rec.price

        if self.dry_run:
            ok = await self._sim.modify(rec, price, size,
                                        self.best_bid or 0.0, self.best_ask or 0.0)
            if ok:
                self.stats.modifies += 1
                log.info(f"{tag}✏️  MOD {side} {old_px:.4f}→{price:.4f}  [SIM]")
            return ok

        if rec.soi is None:
            return False
        try:
            t0 = time.perf_counter_ns()
            api_key_index, nonce = self.client.nonce_manager.next_nonce()
            tx, resp, err = await self.client.modify_order(
                MARKET_INDEX, rec.soi, self._si(size), self._pi(price),
                nonce=nonce, api_key_index=api_key_index,
            )
            us = (time.perf_counter_ns() - t0) // 1_000
            if err:
                log.warning(f"mod {side}: {err}")
                return False
            rec.price     = price
            rec.size      = size
            rec.remaining = max(0.0, size - rec.filled)
            self.stats.modifies += 1
            log.info(f"✏️  MOD {side} {old_px:.4f}→{price:.4f}  {us}µs")
            return True
        except Exception as e:
            log.error(f"mod {side}: {e}")
            return False

    async def _cancel_one(self, rec: Order):
        if rec.status in ("filled", "canceled"):
            return
        side = "ASK" if rec.is_ask else "BID"
        tag  = "[DRY] " if self.dry_run else ""

        if self.dry_run:
            if await self._sim.cancel(rec):
                self.stats.cancels += 1
                log.info(f"{tag}🚫 CANCEL {side} coi={rec.coi}  [SIM]")
            return

        try:
            api_key_index, nonce = self.client.nonce_manager.next_nonce()
            tx, resp, err = await self.client.cancel_order(
                MARKET_INDEX, rec.coi, nonce=nonce, api_key_index=api_key_index,
            )
            if err:
                log.warning(f"cancel {side}: {err}")
            else:
                rec.status = "canceled"
                self.stats.cancels += 1
                log.info(f"🚫 CANCEL {side} coi={rec.coi}")
        except Exception as e:
            log.error(f"cancel: {e}")

    async def _cancel_all_fast(self):
        """Single cancel_all_orders tx (1 round-trip vs 2)."""
        tag = "[DRY] " if self.dry_run else ""
        log.warning(f"{tag}🛑 CANCEL ALL")
        if self.dry_run:
            async with self._olock:
                for rec in (self.bid_ord, self.ask_ord):
                    if rec and rec.status in ("open", "partial"):
                        await self._sim.cancel(rec)
                        self.stats.cancels += 1
            return
        try:
            ts_ms = int(time.time() * 1000)
            await self.client.cancel_all_orders(
                self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME, ts_ms
            )
            async with self._olock:
                for rec in (self.bid_ord, self.ask_ord):
                    if rec and rec.status in ("open", "partial"):
                        rec.status = "canceled"
                        self.stats.cancels += 1
        except Exception as e:
            log.error(f"cancel_all: {e}")

    # ── Regime ───────────────────────────────────────────────────

    def _update_regime(self) -> Regime:
        if self.mid is None:
            return Regime.CALM
        vol = self._vol.update(self.mid)
        if vol >= VOL_SPIKE_BPS:
            return Regime.SPIKE
        elif vol >= VOL_ACTIVE_BPS:
            return Regime.ACTIVE
        return Regime.CALM

    # ── Position sync ─────────────────────────────────────────────

    async def _sync_pos(self):
        if self.dry_run:
            log.info("[DRY] Starting flat")
            return
        try:
            tok, err = self.client.create_auth_token_with_expiry()
            if err:
                log.warning(f"auth: {err}")
                return
            resp = await self.acct_api.account_positions(
                account_index=ACCOUNT_INDEX, authorization=tok, auth=tok,
            )
            for p in (resp.positions or []):
                if int(p.market_id) == MARKET_INDEX:
                    sign     = 1 if int(p.sign) >= 0 else -1
                    self.pos    = float(p.position) * sign
                    self.avg_px = float(p.avg_entry_price)
                    log.info(f"📊 pos={self.pos:+.6f} @ avg {self.avg_px:.4f}")
                    return
        except Exception as e:
            log.warning(f"pos sync: {e}")

    # ── Main loop ─────────────────────────────────────────────────

    async def quoting_loop(self):
        mode = "DRY RUN" if self.dry_run else "LIVE"
        log.info(
            f"🚀 MM start  {mode}  market={MARKET_INDEX}  acct={ACCOUNT_INDEX}  "
            f"half_spread_init={MIN_HALF_SPREAD_BPS}bps  reprice={REPRICE_BPS}bps  "
            f"vol_active={VOL_ACTIVE_BPS}bps  spike={VOL_SPIKE_BPS}bps"
        )

        while self._running:
            try:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                    self._wake.clear()
                except asyncio.TimeoutError:
                    pass

                if not self._running:
                    break

                new_regime = self._update_regime()

                if new_regime == Regime.SPIKE:
                    if self._regime != Regime.SPIKE:
                        log.warning(
                            f"⚡ SPIKE vol={self._vol.value:.1f}bps — "
                            f"cancel all, pause {SPIKE_PAUSE_S}s"
                        )
                        await self._cancel_all_fast()
                        self._regime      = Regime.SPIKE
                        self._spike_until = time.time() + SPIKE_PAUSE_S
                    if time.time() < self._spike_until:
                        continue
                    self._regime = Regime.CALM
                else:
                    self._regime = new_regime

                # Autotune spread
                self._autotune_spread()

                async with self._olock:
                    await self._step()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"loop: {e}", exc_info=True)
                await asyncio.sleep(0.05)

    async def _step(self):
        quotes = self._target_quotes()
        vol    = self._vol.value
        spread = self._spread_bps()
        tag    = "[DRY] " if self.dry_run else ""

        if quotes is None:
            log.debug(
                f"{tag}no quote  book_spread={self._bps(spread)}bps  "
                f"vol={vol:.2f}bps  half={self._half_spread_bps:.2f}bps"
            )
            for rec in (self.bid_ord, self.ask_ord):
                if rec and rec.status in ("open", "partial"):
                    await self._cancel_one(rec)
            self._last_quoted_mid = None
            return

        bid_px, ask_px = quotes
        bid_alive = self.bid_ord and self.bid_ord.status in ("open", "partial")
        ask_alive = self.ask_ord and self.ask_ord.status in ("open", "partial")

        log.debug(
            f"{tag}OB bid={self.best_bid:.4f} ask={self.best_ask:.4f} "
            f"spread={self._bps(spread)}bps  vol={vol:.2f}bps  "
            f"half={self._half_spread_bps:.2f}bps  regime={self._regime.value}  "
            f"→ quot bid={bid_px:.4f} ask={ask_px:.4f}  pos={self.pos:+.6f}"
        )

        # ── Place both fresh ──────────────────────────────────────
        if not bid_alive and not ask_alive:
            if self._inventory_ok(False) and self._inventory_ok(True):
                b, a = await self._place_both(bid_px, ask_px)
                if b: self.bid_ord = b
                if a: self.ask_ord = a
                self._last_quoted_mid = self.mid
            return

        # ── Both alive → batch reprice if moved ──────────────────
        if bid_alive and ask_alive and self._should_reprice():
            ok = await self._modify_both(self.bid_ord, self.ask_ord, bid_px, ask_px)
            if ok:
                self._last_quoted_mid = self.mid
                return
            # Batch modify failed (SOI not yet known) → fall through

        # ── Manage sides individually (fallback / one-sided) ─────
        await self._manage_one(self.bid_ord, False, bid_px)
        await self._manage_one(self.ask_ord, True,  ask_px)
        self._last_quoted_mid = self.mid

    async def _manage_one(self, rec: Optional[Order], is_ask: bool, target_px: float):
        side = "ASK" if is_ask else "BID"
        size = self._order_size(is_ask)

        if not self._inventory_ok(is_ask):
            if rec and rec.status in ("open", "partial"):
                log.info(f"📉 inv limit → cancel {side}")
                await self._cancel_one(rec)
            return

        if rec is None or rec.status in ("filled", "canceled"):
            new = await self._place_one(is_ask, target_px, size)
            if new:
                if is_ask: self.ask_ord = new
                else:      self.bid_ord = new
            return

        if rec.price == 0:
            return

        drift = abs(target_px - rec.price) / rec.price * 10_000
        if drift < REPRICE_BPS:
            return

        new_size = rec.remaining if rec.filled > 0 else size
        can_mod  = self.dry_run or rec.soi is not None
        if can_mod:
            ok = await self._modify_one(rec, target_px, new_size)
            if not ok:
                await self._cancel_one(rec)
                new = await self._place_one(is_ask, target_px, size)
                if new:
                    if is_ask: self.ask_ord = new
                    else:      self.bid_ord = new

    # ── Stats loop ────────────────────────────────────────────────

    async def stats_loop(self):
        while self._running:
            await asyncio.sleep(30)
            tag = "[DRY] " if self.dry_run else ""
            b_s = f"{self.bid_ord.price:.4f}({self.bid_ord.status})" if self.bid_ord else "none"
            a_s = f"{self.ask_ord.price:.4f}({self.ask_ord.status})" if self.ask_ord else "none"
            log.info(f"{tag}📊 {self.stats.line()}")
            log.info(
                f"   pos={self.pos:+.6f}(≈{self.pos*(self.mid or 0):+.2f}USD)  "
                f"half_spread={self._half_spread_bps:.2f}bps  "
                f"regime={self._regime.value}  vol={self._vol.value:.2f}bps  "
                f"bid={b_s}  ask={a_s}"
            )

    # ── Run / shutdown ────────────────────────────────────────────

    async def run(self):
        await self._sync_pos()
        await asyncio.gather(
            asyncio.create_task(self.quoting_loop(), name="mm"),
            asyncio.create_task(self.stats_loop(),   name="stats"),
            return_exceptions=True,
        )

    async def shutdown(self):
        tag = "[DRY] " if self.dry_run else ""
        log.info(f"{tag}shutdown")
        self._running = False
        await self._cancel_all_fast()
        log.info(f"final: {self.stats.line()}")


# ─────────────────────────────────────────────────────────────
#  API KEY LOADER
# ─────────────────────────────────────────────────────────────

def load_api_keys() -> Dict[int, str]:
    keys: Dict[int, str] = {}
    for k, v in os.environ.items():
        if k.startswith("LIGHTER_API_KEY_"):
            try:
                keys[int(k[len("LIGHTER_API_KEY_"):])] = v.strip()
            except ValueError:
                pass
    return keys


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────

async def main():
    global ACCOUNT_INDEX, API_KEYS, BASE_URL

    BASE_URL      = os.getenv("LIGHTER_BASE_URL",      BASE_URL)
    ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", str(ACCOUNT_INDEX)))
    API_KEYS      = load_api_keys()

    if not API_KEYS:
        raise RuntimeError("No API keys. Add LIGHTER_API_KEY_0=<hex> to .env")

    log.info(
        f"keys={sorted(API_KEYS.keys())} acct={ACCOUNT_INDEX} market={MARKET_INDEX} "
        f"dry_run={DRY_RUN}  size={ORDER_SIZE_BASE}  "
        f"half_spread={MIN_HALF_SPREAD_BPS}-{MAX_HALF_SPREAD_BPS}bps  "
        f"reprice={REPRICE_BPS}bps  vol_spike={VOL_SPIKE_BPS}bps"
    )

    api_client = ApiClient(configuration=Configuration(host=BASE_URL))

    if DRY_RUN:
        client = None
        log.info("[DRY] No SignerClient needed")
    else:
        client = lighter.SignerClient(
            url=BASE_URL,
            account_index=ACCOUNT_INDEX,
            api_private_keys=API_KEYS,
        )
        err = client.check_client()
        if err:
            raise RuntimeError(f"API key check failed: {err}")
        log.info("✅ API key OK")

    mm = MarketMaker(client, api_client, dry_run=DRY_RUN)

    ws_host = BASE_URL.replace("https://", "").replace("http://", "").rstrip("/")
    ws = WsClient(
        host=ws_host,
        path="/stream",
        order_book_ids=[MARKET_INDEX],
        account_ids=[] if DRY_RUN else [ACCOUNT_INDEX],
        on_order_book_update=mm.on_ob_update,
        on_account_update=mm.on_account_update,
    )

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _sig():
        log.info("signal → shutdown")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, _sig)
        except NotImplementedError: pass

    ws_thread = threading.Thread(target=ws.run, daemon=True, name="ws")
    ws_thread.start()

    log.info(f"⏳ Waiting for OB tick  market={MARKET_INDEX} ...")
    t0, last_log = time.time(), time.time()
    while mm.mid is None:
        if time.time() - t0 > 30:
            raise RuntimeError(f"Timeout: no OB data after 30s for market {MARKET_INDEX}")
        if time.time() - last_log >= 5:
            log.info(f"  waiting... {time.time()-t0:.0f}s  ws={ws_thread.is_alive()}")
            last_log = time.time()
        await asyncio.sleep(0.05)

    log.info(
        f"✅ OB ready  bid={mm.best_bid:.4f}  ask={mm.best_ask:.4f}  "
        f"spread={mm._bps(mm._spread_bps())}bps"
    )

    mm_task   = asyncio.create_task(mm.run(),    name="mm")
    stop_task = asyncio.create_task(stop.wait(), name="stop")

    done, pending = await asyncio.wait(
        [mm_task, stop_task], return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()

    await mm.shutdown()
    await asyncio.gather(*pending, return_exceptions=True)
    await api_client.close()
    if client:
        await client.close()
    log.info("👋 done")


if __name__ == "__main__":
    asyncio.run(main())
