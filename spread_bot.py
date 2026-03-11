"""
Lighter Perpetual Spread Market Maker Bot
==========================================
Strategy
--------
- Subscribe to live order book via WebSocket (market 92 by default)
- Quote AT the best bid and best ask (not offset from mid)
- Only quote when market spread >= MIN_SPREAD_BPS (default 1 bps)
- On every OB tick: if best bid/ask moved beyond PRICE_MOVE_THRESH_BPS,
  modify existing orders to the new best bid/ask price in-place
- One-sided fill: if bid fills and ask is still open, immediately
  cancel+replace ask at current best ask so we are never unhedged
- Partial fill: track remaining qty, keep the order live at same price,
  only replace when price moves
- Inventory skew: shift quotes when position grows
- Circuit breaker: cancel all if price moves > CIRCUIT_BPS in 1 second
- DRY RUN mode: full simulation, no orders sent (DRY_RUN=true in .env)
- Config from .env file (no external dotenv dependency)
"""

import asyncio
import logging
import time
import os
import signal
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple
from collections import deque
from enum import Enum
from pathlib import Path

try:
    import lighter
    from lighter import ApiClient, WsClient
    from lighter.configuration import Configuration
except ModuleNotFoundError:
    raise SystemExit(
        "\n[ERROR] The 'lighter' SDK is not installed.\n"
        "Run:  pip install lighter-sdk\n"
    )


# ─────────────────────────────────────────────────────────────
#  .ENV LOADER  (stdlib only, no python-dotenv needed)
# ─────────────────────────────────────────────────────────────

def _load_dotenv(path: str = ".env") -> None:
    """Inject KEY=VALUE pairs from .env into os.environ (real env wins)."""
    p = Path(path)
    if not p.exists():
        return
    with open(p) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            if " #" in val:
                val = val[: val.index(" #")].strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            if key and key not in os.environ:
                os.environ[key] = val


_load_dotenv(os.getenv("ENV_FILE", ".env"))


# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────

BASE_URL      = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
API_KEYS: Dict[int, str] = {}   # populated in load_api_keys()

MARKET_INDEX          = int(os.getenv("LIGHTER_MARKET_INDEX",   "92"))
ORDER_SIZE_BASE       = float(os.getenv("ORDER_SIZE_BASE",       "0.01"))
MAX_INVENTORY_BASE    = float(os.getenv("MAX_INVENTORY",         "0.1"))
MIN_SPREAD_BPS        = float(os.getenv("MIN_SPREAD_BPS",        "1.0"))
PRICE_MOVE_THRESH_BPS = float(os.getenv("PRICE_MOVE_THRESH_BPS", "1.0"))
CIRCUIT_BPS           = float(os.getenv("CIRCUIT_BPS",           "50.0"))
MAX_POSITION_USD      = float(os.getenv("MAX_POSITION_USD",       "500.0"))
SKEW_FACTOR           = float(os.getenv("SKEW_FACTOR",            "0.3"))
LOG_LEVEL             = os.getenv("LOG_LEVEL", "INFO")

DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in ("true", "1", "yes")

# Price/size encoding for Lighter API
PRICE_SCALE = 100          # price_int = round(price_float * PRICE_SCALE)
SIZE_SCALE  = 10_000_000   # size_int  = round(size_float  * SIZE_SCALE)


# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("spread_bot")


# ─────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────

class BotState(Enum):
    IDLE     = "idle"
    QUOTING  = "quoting"
    HALTED   = "halted"
    SHUTDOWN = "shutdown"


@dataclass
class OrderRecord:
    client_order_index: int
    server_order_index: Optional[int] = None
    price: float = 0.0
    base_amount: float = 0.0
    is_ask: bool = False
    status: str = "pending"   # pending / open / partial / filled / canceled
    filled_base: float = 0.0
    remaining_base: float = 0.0
    simulated: bool = False


@dataclass
class BotStats:
    orders_placed: int = 0
    total_fills: int = 0
    bid_fills: int = 0
    ask_fills: int = 0
    partial_fills: int = 0
    modifies: int = 0
    cancels: int = 0
    total_volume_base: float = 0.0
    realized_pnl: float = 0.0           # ask_fill_price - bid_fill_price per round-trip
    _last_bid_fill_price: float = 0.0   # internal tracking
    start_time: float = field(default_factory=time.time)

    def record_fill(self, is_ask: bool, price: float, qty: float):
        self.total_fills += 1
        self.total_volume_base += qty
        if is_ask:
            self.ask_fills += 1
            if self._last_bid_fill_price > 0:
                self.realized_pnl += (price - self._last_bid_fill_price) * qty
                self._last_bid_fill_price = 0.0
        else:
            self.bid_fills += 1
            self._last_bid_fill_price = price

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        return (
            f"Runtime={elapsed:.0f}s  "
            f"Placed={self.orders_placed}  "
            f"Fills={self.total_fills}(B={self.bid_fills} A={self.ask_fills})  "
            f"Partials={self.partial_fills}  "
            f"Modifies={self.modifies}  Cancels={self.cancels}  "
            f"Volume={self.total_volume_base:.6f}  "
            f"PnL≈{self.realized_pnl:.4f} USDC"
        )


# ─────────────────────────────────────────────────────────────
#  DRY-RUN SIMULATED EXCHANGE
# ─────────────────────────────────────────────────────────────

class DryRunExchange:
    """
    Simulates the exchange locally. Fires a fill callback if a placed or
    modified order price crosses the live best bid/ask (taker simulation).
    """

    def __init__(self):
        self._orders: Dict[int, OrderRecord] = {}
        self._callbacks = []

    def on_fill(self, coro_fn):
        self._callbacks.append(coro_fn)

    async def _fire(self, rec: OrderRecord, fill_qty: float):
        rec.filled_base    += fill_qty
        rec.remaining_base  = max(0.0, rec.base_amount - rec.filled_base)
        rec.status = "filled" if rec.remaining_base < 1e-9 else "partial"
        update = {
            "client_order_index":    rec.client_order_index,
            "status":                rec.status,
            "filled_base_amount":    rec.filled_base,
            "remaining_base_amount": rec.remaining_base,
            "price":                 str(rec.price),
        }
        for cb in self._callbacks:
            await cb(update)

    async def _check_fill(self, rec: OrderRecord, best_bid: float, best_ask: float):
        if rec.status != "open":
            return
        if rec.is_ask and best_bid > 0 and rec.price <= best_bid:
            await self._fire(rec, rec.remaining_base)
        elif not rec.is_ask and best_ask > 0 and rec.price >= best_ask:
            await self._fire(rec, rec.remaining_base)

    async def place(self, coi: int, price: float, size: float, is_ask: bool,
                    best_bid: float, best_ask: float) -> OrderRecord:
        rec = OrderRecord(
            client_order_index=coi, server_order_index=coi,
            price=price, base_amount=size, is_ask=is_ask,
            status="open", remaining_base=size, simulated=True,
        )
        self._orders[coi] = rec
        await self._check_fill(rec, best_bid, best_ask)
        return rec

    async def modify(self, rec: OrderRecord, new_price: float, new_size: float,
                     best_bid: float, best_ask: float) -> bool:
        if rec.status not in ("open", "partial"):
            return False
        rec.price          = new_price
        rec.base_amount    = new_size
        rec.remaining_base = max(0.0, new_size - rec.filled_base)
        if rec.remaining_base > 0:
            rec.status = "open"
        await self._check_fill(rec, best_bid, best_ask)
        return True

    async def cancel(self, rec: OrderRecord) -> bool:
        if rec.status in ("filled", "canceled"):
            return False
        rec.status = "canceled"
        return True


# ─────────────────────────────────────────────────────────────
#  SPREAD BOT
# ─────────────────────────────────────────────────────────────

class SpreadBot:
    """
    Strategy: quote AT best_bid / best_ask.
    Requote when the price moves more than PRICE_MOVE_THRESH_BPS.
    Handle partial fills by keeping order alive; handle one-sided fills
    by immediately replacing the other leg.
    """

    def __init__(self, client, api_client, dry_run: bool = False):
        self.client     = client
        self.api_client = api_client
        self.dry_run    = dry_run

        if not dry_run:
            self.account_api = lighter.AccountApi(api_client)
        else:
            self.account_api = None

        self._sim = DryRunExchange()
        self._sim.on_fill(self._handle_order_update)

        # Live market data — written by WS thread, read by async loop
        self.best_bid:  Optional[float] = None
        self.best_ask:  Optional[float] = None
        self.mid_price: Optional[float] = None

        # Active orders
        self.bid_order: Optional[OrderRecord] = None
        self.ask_order: Optional[OrderRecord] = None
        self._order_lock = asyncio.Lock()

        # Monotonic client-order-index
        self._coi = int(time.time() * 1000) % (2**32)

        # Inventory
        self.net_position:    float = 0.0
        self.avg_entry_price: float = 0.0

        # Circuit breaker price history
        self._price_hist: deque = deque(maxlen=200)

        self.state   = BotState.IDLE
        self.stats   = BotStats()
        self._wake   = asyncio.Event()
        self._running = True

        if dry_run:
            log.warning(
                "\n"
                "╔══════════════════════════════════════════════════╗\n"
                "║  🧪  DRY RUN — NO ORDERS SENT TO EXCHANGE        ║\n"
                "╚══════════════════════════════════════════════════╝"
            )

    # ── helpers ──────────────────────────────────────────────

    def _ncoi(self) -> int:
        self._coi = (self._coi + 1) & 0x7FFF_FFFF_FFFF
        return self._coi

    @staticmethod
    def _bps(v: Optional[float]) -> str:
        """Safe format for bps values that may be None."""
        return f"{v:.2f}" if v is not None else "n/a"

    def _spread_bps(self) -> Optional[float]:
        if self.best_bid and self.best_ask and self.best_bid > 0:
            return (self.best_ask - self.best_bid) / self.best_bid * 10_000
        return None

    def _inventory_skew(self) -> float:
        """Returns -1..+1 skew. Positive = long, want to sell."""
        if MAX_INVENTORY_BASE == 0:
            return 0.0
        return max(-1.0, min(1.0, self.net_position / MAX_INVENTORY_BASE)) * SKEW_FACTOR

    def _desired_quotes(self) -> Optional[Tuple[float, float]]:
        """
        Returns (bid_price, ask_price) = (best_bid, best_ask).
        Returns None if spread < MIN_SPREAD_BPS.
        Applies inventory skew: tighten the side we want filled.
        """
        if self.best_bid is None or self.best_ask is None:
            return None
        spread = self._spread_bps()
        if spread is None or spread < MIN_SPREAD_BPS:
            return None

        bid_px = self.best_bid
        ask_px = self.best_ask

        # Inventory skew: pull the imbalancing side back slightly
        # so we are less aggressive when position is stretched
        skew = self._inventory_skew()
        tick = 1.0 / PRICE_SCALE
        if skew > 0:
            # Long → make bid less competitive, ask more competitive
            bid_px = round((bid_px - skew * tick) / tick) * tick
        elif skew < 0:
            # Short → make ask less competitive, bid more competitive
            ask_px = round((ask_px - skew * tick) / tick) * tick

        if ask_px <= bid_px:
            return None
        return bid_px, ask_px

    def _circuit_breaker(self) -> bool:
        now = time.time()
        self._price_hist.append((now, self.mid_price))
        window = [p for t, p in self._price_hist if now - t <= 1.0]
        if len(window) < 2 or min(window) == 0:
            return False
        move = (max(window) - min(window)) / min(window) * 10_000
        if move > CIRCUIT_BPS:
            log.warning(f"⚡ CIRCUIT BREAKER: {move:.1f} bps/s — halting all orders")
            return True
        return False

    def _inventory_ok(self, is_ask: bool) -> bool:
        """False if placing this side would push position past limits."""
        if abs(self.net_position) >= MAX_INVENTORY_BASE:
            if is_ask and self.net_position <= -MAX_INVENTORY_BASE:
                return False
            if not is_ask and self.net_position >= MAX_INVENTORY_BASE:
                return False
        if self.mid_price:
            if abs(self.net_position) * self.mid_price >= MAX_POSITION_USD:
                # Only allow orders that reduce position
                if is_ask and self.net_position > 0:
                    return True   # selling reduces long
                if not is_ask and self.net_position < 0:
                    return True   # buying reduces short
                return False
        return True

    def _order_size(self, is_ask: bool) -> float:
        """Scale size based on inventory to fade into imbalance."""
        ratio = self.net_position / MAX_INVENTORY_BASE if MAX_INVENTORY_BASE else 0
        scale = (1.0 + ratio * 0.5) if is_ask else (1.0 - ratio * 0.5)
        return max(0.0, ORDER_SIZE_BASE * max(0.2, min(2.0, scale)))

    # ── WebSocket callbacks ───────────────────────────────────

    def on_order_book_update(self, market_id, order_book: dict):
        try:
            if int(market_id) != MARKET_INDEX:
                return
            asks = [a for a in order_book.get("asks", []) if float(a.get("size", 0)) > 0]
            bids = [b for b in order_book.get("bids", []) if float(b.get("size", 0)) > 0]
            if asks and bids:
                self.best_ask  = float(asks[0]["price"])
                self.best_bid  = float(bids[0]["price"])
                self.mid_price = (self.best_bid + self.best_ask) / 2
                self._wake.set()
        except Exception as e:
            log.debug(f"OB parse error: {e}")

    def on_account_update(self, account_id, account: dict):
        if self.dry_run:
            return
        for o in account.get("orders", []):
            asyncio.get_event_loop().call_soon_threadsafe(
                lambda order=o: asyncio.ensure_future(self._handle_order_update(order))
            )

    # ── Order update handler ──────────────────────────────────

    async def _handle_order_update(self, order: dict):
        """Process fill / cancel / partial update for our orders."""
        coi       = str(order.get("client_order_index") or order.get("client_order_id") or "")
        status    = order.get("status", "")
        filled    = float(order.get("filled_base_amount",    0) or 0)
        remaining = float(order.get("remaining_base_amount", 0) or 0)

        async with self._order_lock:
            for side, rec in [("bid", self.bid_order), ("ask", self.ask_order)]:
                if rec is None or str(rec.client_order_index) != coi:
                    continue

                delta = filled - rec.filled_base
                if delta > 1e-10:
                    rec.filled_base    = filled
                    rec.remaining_base = remaining
                    self.stats.record_fill(rec.is_ask, rec.price, delta)

                    direction = "SHORT" if rec.is_ask else "LONG"
                    tag = "[DRY] " if self.dry_run else ""
                    log.info(
                        f"{tag}✅ FILL {side.upper()} {direction} "
                        f"+{delta:.6f} @ {rec.price:.4f}  "
                        f"net_pos={self.net_position:+.6f}  remain={remaining:.6f}"
                    )
                    if rec.is_ask:
                        self.net_position -= delta
                    else:
                        self.net_position += delta

                if status == "filled":
                    rec.status = "filled"
                    log.info(f"{'[DRY] ' if self.dry_run else ''}🟢 FULL FILL {side.upper()} coi={coi}")
                    asyncio.ensure_future(self._one_sided_fill(side, rec))

                elif status.startswith("canceled"):
                    rec.status = "canceled"
                    self.stats.cancels += 1
                    log.info(f"{'[DRY] ' if self.dry_run else ''}❌ CANCELED {side.upper()} coi={coi}: {status}")

                elif remaining > 0 and remaining < rec.base_amount and rec.status != "partial":
                    rec.status = "partial"
                    self.stats.partial_fills += 1
                    log.info(
                        f"{'[DRY] ' if self.dry_run else ''}⚡ PARTIAL {side.upper()} "
                        f"{filled:.6f}/{rec.base_amount:.6f} @ {rec.price:.4f}  "
                        f"remain={remaining:.6f}"
                    )
                    # Keep order live — do NOT cancel; it will fill or get repriced

    async def _one_sided_fill(self, filled_side: str, rec: OrderRecord):
        """
        One leg fully filled. Immediately cancel+replace the other leg
        at the current best price so we re-establish a balanced quote.
        """
        await asyncio.sleep(0.001)
        async with self._order_lock:
            other_is_ask = (filled_side == "bid")
            other = self.ask_order if other_is_ask else self.bid_order

            if other is None or other.status in ("filled", "canceled"):
                # Other side already gone — just place both fresh next tick
                return

            desired = self._desired_quotes()
            if desired is None:
                await self._cancel_order(other)
                return

            bid_px, ask_px = desired
            new_price = ask_px if other_is_ask else bid_px
            new_size  = self._order_size(other_is_ask)

            # Prefer modify; fall back to cancel+replace
            success = await self._modify_order(other, new_price, new_size)
            if not success:
                await self._cancel_order(other)
                new_rec = await self._place_order(other_is_ask, new_price, new_size)
                if new_rec:
                    if other_is_ask:
                        self.ask_order = new_rec
                    else:
                        self.bid_order = new_rec

    # ── Order actions ─────────────────────────────────────────

    async def _place_order(self, is_ask: bool, price: float, size: float) -> Optional[OrderRecord]:
        coi   = self._ncoi()
        side  = "ASK" if is_ask else "BID"
        tag   = "[DRY] " if self.dry_run else ""
        p_int = round(price * PRICE_SCALE)
        s_int = round(size  * SIZE_SCALE)

        if p_int <= 0 or s_int <= 0:
            return None

        if self.dry_run:
            rec = await self._sim.place(
                coi, price, size, is_ask,
                self.best_bid or 0.0, self.best_ask or 0.0,
            )
            self.stats.orders_placed += 1
            log.info(f"{tag}📤 PLACE {side}  coi={coi}  price={price:.4f}  size={size:.6f}  [SIM]")
            return rec

        try:
            t0 = time.perf_counter_ns()
            tx, resp, err = await self.client.create_order(
                market_index=MARKET_INDEX,
                client_order_index=coi,
                base_amount=s_int,
                price=p_int,
                is_ask=is_ask,
                order_type=self.client.ORDER_TYPE_LIMIT,
                time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            us = (time.perf_counter_ns() - t0) // 1_000
            if err:
                log.warning(f"PLACE {side} err: {err}")
                return None
            rec = OrderRecord(
                client_order_index=coi, price=price,
                base_amount=size, is_ask=is_ask,
                status="open", remaining_base=size,
            )
            self.stats.orders_placed += 1
            log.info(f"📤 PLACE {side}  coi={coi}  price={price:.4f}  size={size:.6f}  {us}µs")
            return rec
        except Exception as e:
            log.error(f"PLACE {side} exception: {e}")
            return None

    async def _modify_order(self, rec: OrderRecord, new_price: float, new_size: float) -> bool:
        if rec.status not in ("open", "partial"):
            return False
        side     = "ASK" if rec.is_ask else "BID"
        old_px   = rec.price
        tag      = "[DRY] " if self.dry_run else ""
        new_size = max(new_size, rec.remaining_base)   # never reduce below remaining
        p_int    = round(new_price * PRICE_SCALE)
        s_int    = round(new_size  * SIZE_SCALE)

        if p_int <= 0 or s_int <= 0:
            return False

        if self.dry_run:
            ok = await self._sim.modify(
                rec, new_price, new_size,
                self.best_bid or 0.0, self.best_ask or 0.0,
            )
            if ok:
                self.stats.modifies += 1
                log.info(f"{tag}✏️  MODIFY {side}  {old_px:.4f}→{new_price:.4f}  size={new_size:.6f}  [SIM]")
            return ok

        if rec.server_order_index is None:
            return False
        try:
            t0 = time.perf_counter_ns()
            api_key_index, nonce = self.client.nonce_manager.next_nonce()
            tx, resp, err = await self.client.modify_order(
                market_index=MARKET_INDEX,
                order_index=rec.server_order_index,
                base_amount=s_int,
                price=p_int,
                trigger_price=0,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            us = (time.perf_counter_ns() - t0) // 1_000
            if err:
                log.warning(f"MODIFY {side} err: {err}")
                return False
            rec.price          = new_price
            rec.base_amount    = new_size
            rec.remaining_base = max(0.0, new_size - rec.filled_base)
            self.stats.modifies += 1
            log.info(f"✏️  MODIFY {side}  {old_px:.4f}→{new_price:.4f}  size={new_size:.6f}  {us}µs")
            return True
        except Exception as e:
            log.error(f"MODIFY exception: {e}")
            return False

    async def _cancel_order(self, rec: OrderRecord):
        if rec.status in ("filled", "canceled"):
            return
        side = "ASK" if rec.is_ask else "BID"
        tag  = "[DRY] " if self.dry_run else ""

        if self.dry_run:
            ok = await self._sim.cancel(rec)
            if ok:
                self.stats.cancels += 1
                log.info(f"{tag}🚫 CANCEL {side}  coi={rec.client_order_index}  [SIM]")
            return

        try:
            api_key_index, nonce = self.client.nonce_manager.next_nonce()
            tx, resp, err = await self.client.cancel_order(
                market_index=MARKET_INDEX,
                order_index=rec.client_order_index,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            if err:
                log.warning(f"CANCEL {side} err: {err}")
            else:
                rec.status = "canceled"
                self.stats.cancels += 1
                log.info(f"🚫 CANCEL {side}  coi={rec.client_order_index}")
        except Exception as e:
            log.error(f"CANCEL exception: {e}")

    async def _cancel_all(self):
        tag = "[DRY] " if self.dry_run else ""
        log.warning(f"{tag}🛑 Canceling all open orders")
        tasks = []
        async with self._order_lock:
            for rec in (self.bid_order, self.ask_order):
                if rec and rec.status in ("open", "partial"):
                    tasks.append(self._cancel_order(rec))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Position sync ─────────────────────────────────────────

    async def _sync_position(self):
        if self.dry_run:
            log.info("[DRY] Starting flat (no position sync in dry run)")
            return
        try:
            tok, err = self.client.create_auth_token_with_expiry()
            if err:
                log.warning(f"Auth token err: {err}")
                return
            resp = await self.account_api.account_positions(
                account_index=ACCOUNT_INDEX, authorization=tok, auth=tok,
            )
            for pos in (resp.positions or []):
                if int(pos.market_id) == MARKET_INDEX:
                    sign = 1 if int(pos.sign) >= 0 else -1
                    self.net_position    = float(pos.position) * sign
                    self.avg_entry_price = float(pos.avg_entry_price)
                    log.info(f"📊 Synced pos: {self.net_position:+.6f} @ avg {self.avg_entry_price:.4f}")
                    return
        except Exception as e:
            log.warning(f"Position sync failed: {e}")

    # ── Quoting loop ──────────────────────────────────────────

    async def quoting_loop(self):
        mode = "DRY RUN" if self.dry_run else "LIVE"
        log.info(f"🚀 Quoting  mode={mode}  market={MARKET_INDEX}  account={ACCOUNT_INDEX}")
        self.state = BotState.QUOTING

        while self._running:
            try:
                # Wait for next OB tick (or heartbeat timeout)
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1.0)
                    self._wake.clear()
                except asyncio.TimeoutError:
                    pass

                if not self._running:
                    break

                # Circuit breaker
                if self.mid_price and self._circuit_breaker():
                    if self.state != BotState.HALTED:
                        self.state = BotState.HALTED
                        await self._cancel_all()
                    continue

                # Auto-resume
                if self.state == BotState.HALTED:
                    recent = [p for t, p in self._price_hist if time.time() - t < 5]
                    if len(recent) >= 2 and min(recent) > 0:
                        if (max(recent) - min(recent)) / min(recent) * 10_000 < CIRCUIT_BPS * 0.4:
                            log.info("✅ Circuit breaker reset")
                            self.state = BotState.QUOTING
                    continue

                async with self._order_lock:
                    await self._quoting_step()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Quoting loop error: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    async def _quoting_step(self):
        tag    = "[DRY] " if self.dry_run else ""
        spread = self._spread_bps()
        desired = self._desired_quotes()

        # ── No opportunity ────────────────────────────────────
        if desired is None:
            log.debug(f"{tag}spread={self._bps(spread)}bps < {MIN_SPREAD_BPS}bps, no quote")
            for rec in (self.bid_order, self.ask_order):
                if rec and rec.status in ("open", "partial"):
                    await self._cancel_order(rec)
            return

        bid_px, ask_px = desired
        log.debug(
            f"{tag}OB bid={self.best_bid:.4f}  ask={self.best_ask:.4f}  "
            f"spread={self._bps(spread)}bps  "
            f"→ quote bid={bid_px:.4f}  ask={ask_px:.4f}  "
            f"pos={self.net_position:+.6f}  skew={self._inventory_skew():+.3f}"
        )

        await self._manage_side(self.bid_order, is_ask=False, target_price=bid_px)
        await self._manage_side(self.ask_order, is_ask=True,  target_price=ask_px)

    async def _manage_side(
        self,
        current: Optional[OrderRecord],
        is_ask: bool,
        target_price: float,
    ):
        side = "ASK" if is_ask else "BID"
        size = self._order_size(is_ask)

        if size < 1e-9:
            return

        # Hard inventory check
        if not self._inventory_ok(is_ask):
            if current and current.status in ("open", "partial"):
                log.info(f"📉 Inventory limit → cancel {side}")
                await self._cancel_order(current)
            return

        # No order → place fresh
        if current is None or current.status in ("filled", "canceled"):
            rec = await self._place_order(is_ask, target_price, size)
            if rec:
                if is_ask:
                    self.ask_order = rec
                else:
                    self.bid_order = rec
            return

        # Order exists but partially filled → keep it, only reprice on move
        # Order exists and open → reprice if moved enough
        if current.price == 0:
            return

        drift_bps = abs(target_price - current.price) / current.price * 10_000
        if drift_bps < PRICE_MOVE_THRESH_BPS:
            return   # price hasn't moved enough — leave order alone

        # Price moved: modify in-place (preserves queue priority on partial)
        # For a partial fill, keep remaining size; for untouched, use full size
        new_size  = current.remaining_base if current.filled_base > 0 else size
        can_modify = self.dry_run or current.server_order_index is not None

        if can_modify:
            ok = await self._modify_order(current, target_price, new_size)
            if not ok:
                # Modify failed — cancel and replace
                await self._cancel_order(current)
                rec = await self._place_order(is_ask, target_price, size)
                if rec:
                    if is_ask:
                        self.ask_order = rec
                    else:
                        self.bid_order = rec
        else:
            log.debug(f"{side} modify skipped — server_index not yet known (in-flight)")

    # ── Stats loop ────────────────────────────────────────────

    async def stats_loop(self):
        while self._running:
            await asyncio.sleep(30)
            tag = "[DRY] " if self.dry_run else ""
            bid_s = f"{self.bid_order.price:.4f}({self.bid_order.status})" if self.bid_order else "none"
            ask_s = f"{self.ask_order.price:.4f}({self.ask_order.status})" if self.ask_order else "none"
            log.info(f"{tag}📊 {self.stats.summary()}")
            log.info(
                f"   pos={self.net_position:+.6f} base "
                f"(≈{self.net_position * (self.mid_price or 0):+.2f} USD)  "
                f"state={self.state.value}  bid={bid_s}  ask={ask_s}"
            )

    # ── Run / shutdown ────────────────────────────────────────

    async def run(self):
        await self._sync_position()
        await asyncio.gather(
            asyncio.create_task(self.quoting_loop(), name="quoting"),
            asyncio.create_task(self.stats_loop(),   name="stats"),
            return_exceptions=True,
        )

    async def shutdown(self):
        tag = "[DRY] " if self.dry_run else ""
        log.info(f"{tag}Shutting down...")
        self._running = False
        self.state    = BotState.SHUTDOWN
        await self._cancel_all()
        log.info(f"Final: {self.stats.summary()}")


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
        raise RuntimeError(
            "No API keys found.\n"
            "Add LIGHTER_API_KEY_0=<hex> to your .env file."
        )

    log.info(f"Keys: {sorted(API_KEYS.keys())}  account={ACCOUNT_INDEX}  market={MARKET_INDEX}")
    log.info(f"URL: {BASE_URL}  dry_run={DRY_RUN}")

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
        log.info("✅ API key check passed")

    bot = SpreadBot(client, api_client, dry_run=DRY_RUN)

    ws_host = BASE_URL.replace("https://", "").replace("http://", "").rstrip("/")
    ws = WsClient(
        host=ws_host,
        path="/stream",
        order_book_ids=[MARKET_INDEX],
        account_ids=[] if DRY_RUN else [ACCOUNT_INDEX],
        on_order_book_update=bot.on_order_book_update,
        on_account_update=bot.on_account_update,
    )

    loop           = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _sig():
        log.info("Signal — shutting down")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except NotImplementedError:
            pass

    ws_thread = threading.Thread(target=ws.run, daemon=True, name="ws")
    ws_thread.start()

    log.info(f"⏳ Waiting for OB data from wss://{ws_host}/stream  market={MARKET_INDEX} ...")
    t0       = time.time()
    last_log = t0
    while bot.mid_price is None:
        elapsed = time.time() - t0
        if elapsed > 30:
            raise RuntimeError(
                f"Timeout: no OB data after 30s. "
                f"Check connectivity and that market {MARKET_INDEX} exists."
            )
        if time.time() - last_log >= 5:
            log.info(f"  still waiting... {elapsed:.0f}s  ws_alive={ws_thread.is_alive()}")
            last_log = time.time()
        await asyncio.sleep(0.05)

    log.info(
        f"✅ OB ready  bid={bot.best_bid:.4f}  ask={bot.best_ask:.4f}  "
        f"mid={bot.mid_price:.4f}  spread={bot._bps(bot._spread_bps())}bps"
    )

    bot_task  = asyncio.create_task(bot.run(),             name="bot")
    sig_task  = asyncio.create_task(shutdown_event.wait(), name="shutdown")

    done, pending = await asyncio.wait(
        [bot_task, sig_task], return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()

    # Give bot a clean shutdown
    await bot.shutdown()
    await asyncio.gather(*pending, return_exceptions=True)
    await api_client.close()
    if client:
        await client.close()
    log.info("👋 Done")


if __name__ == "__main__":
    asyncio.run(main())
