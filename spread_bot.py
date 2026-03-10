"""
Lighter Perpetual Spread Market Maker Bot
==========================================
- Zero trading fees assumed (maker_fee = 0)
- Monitors bid/ask spread via WebSocket
- Places parallel bid+ask limit orders when spread > 0.01%
- Modifies orders on price movement (no cancel+repost = saves latency)
- Handles partial fills, one-sided fills, inventory risk
- Circuit breakers for sudden price moves
- Sub-millisecond reaction loop via asyncio
- DRY RUN mode: full simulation with no orders sent (set DRY_RUN=true)
- Config loaded from .env file
"""

import asyncio
import logging
import time
import json
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
        "Docs: https://github.com/lighter-xyz/lighter-v2-python\n"
    )


# ─────────────────────────────────────────────
#  .ENV LOADER  (no external dependency)
# ─────────────────────────────────────────────

def _load_dotenv(path: str = ".env") -> None:
    """
    Parse KEY=VALUE lines from a .env file and inject into os.environ.
    - Ignores blank lines and lines starting with #
    - Strips surrounding quotes (single or double) from values
    - Does NOT override variables already set in the real environment
    """
    env_path = Path(path)
    if not env_path.exists():
        return
    with open(env_path) as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, raw_value = line.partition("=")
            key   = key.strip()
            value = raw_value.strip()
            # Strip inline comments
            if " #" in value:
                value = value[: value.index(" #")].strip()
            # Strip surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            # Real env vars take priority over .env
            if key and key not in os.environ:
                os.environ[key] = value


# Load .env before reading any os.getenv calls below
_load_dotenv(os.getenv("ENV_FILE", ".env"))


# ─────────────────────────────────────────────
#  CONFIGURATION  (reads from env / .env)
# ─────────────────────────────────────────────

BASE_URL          = os.getenv("LIGHTER_BASE_URL",   "https://mainnet.zklighter.elliot.ai")
WS_URL            = os.getenv("LIGHTER_WS_URL",     "wss://mainnet.zklighter.elliot.ai/stream")
ACCOUNT_INDEX     = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
API_KEYS: Dict[int, str] = {}   # populated in load_api_keys()

MARKET_INDEX          = int(os.getenv("LIGHTER_MARKET_INDEX",    "0"))
ORDER_SIZE_BASE       = float(os.getenv("ORDER_SIZE_BASE",        "0.01"))
MAX_INVENTORY_BASE    = float(os.getenv("MAX_INVENTORY",          "0.1"))
MIN_SPREAD_BPS        = float(os.getenv("MIN_SPREAD_BPS",         "1.0"))
QUOTE_OFFSET_BPS      = float(os.getenv("QUOTE_OFFSET_BPS",       "0.5"))
PRICE_MOVE_THRESH_BPS = float(os.getenv("PRICE_MOVE_THRESH_BPS",  "2.0"))
CIRCUIT_BPS           = float(os.getenv("CIRCUIT_BPS",            "50.0"))
MAX_POSITION_USD      = float(os.getenv("MAX_POSITION_USD",        "500.0"))
SKEW_FACTOR           = float(os.getenv("SKEW_FACTOR",             "0.3"))
LOG_LEVEL             = os.getenv("LOG_LEVEL", "INFO")

DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in ("true", "1", "yes")

# Precision (ETH defaults — can be fetched from market config at startup)
PRICE_SCALE = 100          # price_int  = price_float * PRICE_SCALE
SIZE_SCALE  = 10_000_000   # size_int   = size_float  * SIZE_SCALE


# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("spread_bot")


# ─────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────

class BotState(Enum):
    IDLE     = "idle"
    QUOTING  = "quoting"
    HALTED   = "halted"    # circuit breaker active
    SHUTDOWN = "shutdown"


@dataclass
class OrderRecord:
    client_order_index: int
    server_order_index: Optional[int] = None
    price: float = 0.0
    base_amount: float = 0.0
    is_ask: bool = False
    status: str = "pending"        # pending / open / partial / filled / canceled
    filled_base: float = 0.0
    remaining_base: float = 0.0
    simulated: bool = False        # True when created in dry-run mode


@dataclass
class BotStats:
    total_fills: int = 0
    total_pnl_usdc: float = 0.0
    total_volume_base: float = 0.0
    bid_fills: int = 0
    ask_fills: int = 0
    partial_fills: int = 0
    cancels: int = 0
    modifies: int = 0
    orders_placed: int = 0
    start_time: float = field(default_factory=time.time)

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        return (
            f"Runtime={elapsed:.0f}s  "
            f"Placed={self.orders_placed}  "
            f"Fills={self.total_fills}(bid={self.bid_fills} ask={self.ask_fills})  "
            f"Partials={self.partial_fills}  "
            f"Modifies={self.modifies}  "
            f"Cancels={self.cancels}  "
            f"Volume={self.total_volume_base:.6f}  "
            f"PnL≈{self.total_pnl_usdc:.4f} USDC"
        )


# ─────────────────────────────────────────────
#  DRY-RUN SIMULATED EXCHANGE
# ─────────────────────────────────────────────

class DryRunExchange:
    """
    Simulates order placement, modification, and cancellation locally.
    If a placed / modified order's price crosses the live best bid/ask,
    an immediate fill is simulated so post-fill logic can be exercised
    during testing.
    """

    def __init__(self):
        self._orders: Dict[int, OrderRecord] = {}
        self._fill_callbacks = []

    def register_fill_callback(self, coro_fn):
        self._fill_callbacks.append(coro_fn)

    async def _maybe_simulate_fill(
        self, rec: OrderRecord, best_bid: float, best_ask: float
    ):
        if rec.status not in ("open",):
            return
        # Aggressive price → would cross book and fill immediately as taker
        if rec.is_ask and best_bid > 0 and rec.price <= best_bid:
            await self._do_fill(rec, rec.remaining_base)
        elif not rec.is_ask and best_ask > 0 and rec.price >= best_ask:
            await self._do_fill(rec, rec.remaining_base)

    async def _do_fill(self, rec: OrderRecord, fill_qty: float):
        rec.filled_base    += fill_qty
        rec.remaining_base  = max(0.0, rec.base_amount - rec.filled_base)
        rec.status          = "filled" if rec.remaining_base < 1e-9 else "partial"
        update = {
            "client_order_index": rec.client_order_index,
            "status":             rec.status,
            "filled_base_amount":    rec.filled_base,
            "remaining_base_amount": rec.remaining_base,
            "price":              str(rec.price),
        }
        for cb in self._fill_callbacks:
            await cb(update)

    async def place_order(
        self,
        coi: int,
        price: float,
        size: float,
        is_ask: bool,
        best_bid: float,
        best_ask: float,
    ) -> OrderRecord:
        rec = OrderRecord(
            client_order_index=coi,
            server_order_index=coi,   # use COI as synthetic server ID
            price=price,
            base_amount=size,
            is_ask=is_ask,
            status="open",
            remaining_base=size,
            simulated=True,
        )
        self._orders[coi] = rec
        await self._maybe_simulate_fill(rec, best_bid, best_ask)
        return rec

    async def modify_order(
        self,
        rec: OrderRecord,
        new_price: float,
        new_size: float,
        best_bid: float,
        best_ask: float,
    ) -> bool:
        if rec.status not in ("open", "partial"):
            return False
        rec.price          = new_price
        rec.base_amount    = new_size
        rec.remaining_base = max(0.0, new_size - rec.filled_base)
        await self._maybe_simulate_fill(rec, best_bid, best_ask)
        return True

    async def cancel_order(self, rec: OrderRecord) -> bool:
        if rec.status in ("filled", "canceled"):
            return False
        rec.status = "canceled"
        return True


# ─────────────────────────────────────────────
#  SPREAD BOT CORE
# ─────────────────────────────────────────────

class SpreadBot:
    def __init__(self, client, api_client, dry_run: bool = False):
        self.client     = client
        self.api_client = api_client
        self.dry_run    = dry_run

        if not dry_run:
            self.order_api   = lighter.OrderApi(api_client)
            self.account_api = lighter.AccountApi(api_client)
        else:
            self.order_api   = None
            self.account_api = None

        # Dry-run simulated exchange
        self._sim = DryRunExchange()
        self._sim.register_fill_callback(self._handle_order_update)

        # Live market data (updated from WS callback thread)
        self.best_bid:  Optional[float] = None
        self.best_ask:  Optional[float] = None
        self.mid_price: Optional[float] = None

        # Active orders
        self.bid_order: Optional[OrderRecord] = None
        self.ask_order: Optional[OrderRecord] = None
        self._order_lock = asyncio.Lock()

        # Monotonically incrementing client order index
        self._coi_counter = int(time.time() * 1000) % (2**32)

        # Inventory
        self.net_position:    float = 0.0
        self.avg_entry_price: float = 0.0

        # Circuit breaker history
        self._price_history: deque = deque(maxlen=200)

        # State
        self.state  = BotState.IDLE
        self.stats  = BotStats()
        self._ob_update_event = asyncio.Event()
        self._running = True

        if dry_run:
            log.warning(
                "\n"
                "╔══════════════════════════════════════════════════╗\n"
                "║  🧪  DRY RUN MODE — NO ORDERS WILL BE SENT       ║\n"
                "║     All actions are simulated locally.            ║\n"
                "║     Safe to run without real funds.               ║\n"
                "╚══════════════════════════════════════════════════╝"
            )

    # ── helpers ──────────────────────────────────────────────

    def _next_coi(self) -> int:
        self._coi_counter = (self._coi_counter + 1) & 0x7FFF_FFFF_FFFF
        return self._coi_counter

    def _to_price_int(self, price: float) -> int:
        return round(price * PRICE_SCALE)

    def _to_size_int(self, size: float) -> int:
        return round(size * SIZE_SCALE)

    def _spread_bps(self) -> Optional[float]:
        if self.best_bid and self.best_ask and self.best_bid > 0:
            return (self.best_ask - self.best_bid) / self.best_bid * 10_000
        return None

    def _inventory_skew(self) -> float:
        if MAX_INVENTORY_BASE == 0:
            return 0.0
        ratio = self.net_position / MAX_INVENTORY_BASE
        return max(-1.0, min(1.0, ratio)) * SKEW_FACTOR

    def _desired_quotes(self) -> Optional[Tuple[float, float]]:
        if self.mid_price is None:
            return None
        spread = self._spread_bps()
        if spread is None or spread < MIN_SPREAD_BPS:
            return None

        skew         = self._inventory_skew()
        offset_price = self.mid_price * QUOTE_OFFSET_BPS / 10_000

        bid_price = self.mid_price - offset_price - skew * offset_price
        ask_price = self.mid_price + offset_price - skew * offset_price

        # Never cross the live book
        bid_price = min(bid_price, self.best_bid)
        ask_price = max(ask_price, self.best_ask)

        # Round to tick
        tick      = 1.0 / PRICE_SCALE
        bid_price = round(bid_price / tick) * tick
        ask_price = round(ask_price / tick) * tick

        if ask_price <= bid_price:
            return None
        return bid_price, ask_price

    def _check_circuit_breaker(self) -> bool:
        now = time.time()
        self._price_history.append((now, self.mid_price))
        recent = [p for t, p in self._price_history if now - t <= 1.0]
        if len(recent) < 2:
            return False
        lo, hi = min(recent), max(recent)
        if lo == 0:
            return False
        move_bps = (hi - lo) / lo * 10_000
        if move_bps > CIRCUIT_BPS:
            log.warning(f"⚡ CIRCUIT BREAKER: {move_bps:.1f} bps/s — halting!")
            return True
        return False

    def _inventory_ok(self, is_ask: bool) -> bool:
        if abs(self.net_position) > MAX_INVENTORY_BASE:
            if is_ask     and self.net_position < 0:
                return False
            if not is_ask and self.net_position > 0:
                return False
        if self.mid_price:
            pos_usd = abs(self.net_position) * self.mid_price
            if pos_usd >= MAX_POSITION_USD:
                if (is_ask and self.net_position > 0) or (not is_ask and self.net_position < 0):
                    return False
        return True

    def _adjusted_size(self, is_ask: bool) -> float:
        base  = ORDER_SIZE_BASE
        ratio = self.net_position / MAX_INVENTORY_BASE if MAX_INVENTORY_BASE else 0
        scale = (1.0 + ratio * 0.5) if is_ask else (1.0 - ratio * 0.5)
        return max(0.0, base * max(0.2, min(2.0, scale)))

    # ── WebSocket callbacks ───────────────────────────────────

    def on_order_book_update(self, market_id: int, order_book: dict):
        if market_id != MARKET_INDEX:
            return
        try:
            asks = order_book.get("asks", [])
            bids = order_book.get("bids", [])
            if asks and bids:
                self.best_ask  = float(asks[0]["price"])
                self.best_bid  = float(bids[0]["price"])
                self.mid_price = (self.best_bid + self.best_ask) / 2
                self._ob_update_event.set()
        except Exception as e:
            log.debug(f"OB parse error: {e}")

    def on_account_update(self, account_id: int, account: dict):
        if self.dry_run:
            return   # fills handled by DryRunExchange directly
        orders = account.get("orders", [])
        for o in orders:
            asyncio.get_event_loop().call_soon_threadsafe(
                lambda order=o: asyncio.ensure_future(self._handle_order_update(order))
            )

    # ── Order lifecycle ───────────────────────────────────────

    async def _handle_order_update(self, order: dict):
        coi       = order.get("client_order_index") or order.get("client_order_id")
        status    = order.get("status", "")
        remaining = float(order.get("remaining_base_amount", 0))
        filled    = float(order.get("filled_base_amount",    0))

        async with self._order_lock:
            for side, rec in [("bid", self.bid_order), ("ask", self.ask_order)]:
                if rec is None:
                    continue
                if str(rec.client_order_index) != str(coi):
                    continue

                new_fill = filled - rec.filled_base
                if new_fill > 0:
                    rec.filled_base    = filled
                    rec.remaining_base = remaining
                    self.stats.total_volume_base += new_fill
                    self.stats.total_fills       += 1

                    if side == "bid":
                        self.stats.bid_fills += 1
                        self.net_position    += new_fill
                    else:
                        self.stats.ask_fills += 1
                        self.net_position    -= new_fill

                    tag = "[DRY] " if self.dry_run else ""
                    log.info(
                        f"{tag}✅ FILL {side.upper()} +{new_fill:.6f} @ {rec.price:.4f}  "
                        f"net_pos={self.net_position:+.6f}  remain={remaining:.6f}"
                    )

                if status == "filled":
                    rec.status = "filled"
                    log.info(
                        f"{'[DRY] ' if self.dry_run else ''}🟢 FULLY FILLED {side.upper()} "
                        f"coi={coi}"
                    )
                    asyncio.ensure_future(self._on_full_fill(side, rec))

                elif status.startswith("canceled"):
                    rec.status = "canceled"
                    self.stats.cancels += 1

                elif remaining < rec.base_amount and remaining > 0:
                    if rec.status != "partial":
                        rec.status = "partial"
                        self.stats.partial_fills += 1
                        log.info(
                            f"{'[DRY] ' if self.dry_run else ''}⚡ PARTIAL {side.upper()} "
                            f"{filled:.6f}/{rec.base_amount:.6f} @ {rec.price:.4f}"
                        )

    async def _on_full_fill(self, filled_side: str, rec: OrderRecord):
        """Aggressively re-price the opposite leg after a full fill."""
        await asyncio.sleep(0.001)
        async with self._order_lock:
            other_is_ask = (filled_side == "bid")
            other        = self.ask_order if other_is_ask else self.bid_order

            if other is None or other.status in ("filled", "canceled"):
                return
            desired = self._desired_quotes()
            if desired is None:
                return

            bid_px, ask_px = desired
            new_price = ask_px if other_is_ask else bid_px
            skew = self._inventory_skew()

            if abs(skew) > 0.5:
                aggression = 1 - abs(skew) * 0.4
                if other_is_ask:
                    new_price = new_price * aggression + self.mid_price * (1 - aggression)
                else:
                    inv_agg   = 1.0 / aggression if aggression > 0 else 1.0
                    new_price = new_price * inv_agg + self.mid_price * (1 - inv_agg)
                tick      = 1.0 / PRICE_SCALE
                new_price = round(new_price / tick) * tick

            await self._modify_order(other, new_price, other.remaining_base)

    async def _place_order(
        self, is_ask: bool, price: float, size: float
    ) -> Optional[OrderRecord]:
        coi       = self._next_coi()
        price_int = self._to_price_int(price)
        size_int  = self._to_size_int(size)
        side_name = "ASK" if is_ask else "BID"
        tag       = "[DRY] " if self.dry_run else ""

        if size_int <= 0 or price_int <= 0:
            return None

        # ── Dry-run path ──────────────────────────────────────────
        if self.dry_run:
            rec = await self._sim.place_order(
                coi=coi,
                price=price,
                size=size,
                is_ask=is_ask,
                best_bid=self.best_bid or 0.0,
                best_ask=self.best_ask or 0.0,
            )
            self.stats.orders_placed += 1
            log.info(
                f"{tag}📤 PLACE {side_name}  coi={coi}  "
                f"price={price:.4f}  size={size:.6f}  "
                f"spread={self._spread_bps():.2f}bps  "
                f"[SIMULATED — no tx sent]"
            )
            return rec

        # ── Live path ─────────────────────────────────────────────
        try:
            t0 = time.perf_counter_ns()
            tx, resp, err = await self.client.create_order(
                market_index=MARKET_INDEX,
                client_order_index=coi,
                base_amount=size_int,
                price=price_int,
                is_ask=is_ask,
                order_type=self.client.ORDER_TYPE_LIMIT,
                time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            latency_us = (time.perf_counter_ns() - t0) // 1000

            if err:
                log.warning(f"Place {side_name} failed: {err}")
                return None

            rec = OrderRecord(
                client_order_index=coi,
                price=price,
                base_amount=size,
                is_ask=is_ask,
                status="open",
                remaining_base=size,
            )
            self.stats.orders_placed += 1
            log.info(
                f"📤 PLACE {side_name}  coi={coi}  "
                f"price={price:.4f}  size={size:.6f}  latency={latency_us}µs"
            )
            return rec

        except Exception as e:
            log.error(f"Exception placing order: {e}")
            return None

    async def _modify_order(
        self, rec: OrderRecord, new_price: float, new_size: float
    ) -> bool:
        if rec.status not in ("open", "partial"):
            return False

        side_name = "ASK" if rec.is_ask else "BID"
        old_price = rec.price
        tag       = "[DRY] " if self.dry_run else ""

        # ── Dry-run path ──────────────────────────────────────────
        if self.dry_run:
            success = await self._sim.modify_order(
                rec=rec,
                new_price=new_price,
                new_size=new_size,
                best_bid=self.best_bid or 0.0,
                best_ask=self.best_ask or 0.0,
            )
            if success:
                self.stats.modifies += 1
                log.info(
                    f"{tag}✏️  MODIFY {side_name}  "
                    f"{old_price:.4f}→{new_price:.4f}  size={new_size:.6f}  "
                    f"[SIMULATED — no tx sent]"
                )
            return success

        # ── Live path ─────────────────────────────────────────────
        if rec.server_order_index is None:
            log.debug(f"Cannot modify {side_name} — server index not yet known")
            return False

        price_int = self._to_price_int(new_price)
        size_int  = self._to_size_int(new_size)
        if size_int <= 0 or price_int <= 0:
            return False

        try:
            t0 = time.perf_counter_ns()
            api_key_index, nonce = self.client.nonce_manager.next_nonce()
            tx, resp, err = await self.client.modify_order(
                market_index=MARKET_INDEX,
                order_index=rec.server_order_index,
                base_amount=size_int,
                price=price_int,
                trigger_price=0,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            latency_us = (time.perf_counter_ns() - t0) // 1000

            if err:
                log.warning(f"Modify {side_name} failed: {err}")
                return False

            rec.price          = new_price
            rec.base_amount    = new_size
            rec.remaining_base = max(0.0, new_size - rec.filled_base)
            self.stats.modifies += 1
            log.info(
                f"✏️  MODIFY {side_name}  "
                f"{old_price:.4f}→{new_price:.4f}  size={new_size:.6f}  latency={latency_us}µs"
            )
            return True

        except Exception as e:
            log.error(f"Exception modifying order: {e}")
            return False

    async def _cancel_order(self, rec: OrderRecord):
        if rec.status in ("filled", "canceled"):
            return
        side_name = "ASK" if rec.is_ask else "BID"
        tag       = "[DRY] " if self.dry_run else ""

        # ── Dry-run path ──────────────────────────────────────────
        if self.dry_run:
            success = await self._sim.cancel_order(rec)
            if success:
                self.stats.cancels += 1
                log.info(
                    f"{tag}🚫 CANCEL {side_name}  coi={rec.client_order_index}  "
                    f"[SIMULATED — no tx sent]"
                )
            return

        # ── Live path ─────────────────────────────────────────────
        try:
            api_key_index, nonce = self.client.nonce_manager.next_nonce()
            tx, resp, err = await self.client.cancel_order(
                market_index=MARKET_INDEX,
                order_index=rec.client_order_index,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            if err:
                log.warning(f"Cancel {side_name} error: {err}")
            else:
                rec.status = "canceled"
                self.stats.cancels += 1
                log.info(f"🚫 CANCEL {side_name}  coi={rec.client_order_index}")
        except Exception as e:
            log.error(f"Exception canceling: {e}")

    async def _cancel_all(self):
        tag = "[DRY] " if self.dry_run else ""
        log.warning(f"{tag}🛑 Canceling ALL open orders")
        tasks = []
        async with self._order_lock:
            if self.bid_order and self.bid_order.status in ("open", "partial"):
                tasks.append(self._cancel_order(self.bid_order))
            if self.ask_order and self.ask_order.status in ("open", "partial"):
                tasks.append(self._cancel_order(self.ask_order))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Position sync ─────────────────────────────────────────

    async def _sync_position_from_api(self):
        if self.dry_run:
            log.info("[DRY] Skipping live position sync — starting flat at 0")
            return
        try:
            auth_token, err = self.client.create_auth_token_with_expiry()
            if err:
                log.warning(f"Auth token error: {err}")
                return
            resp = await self.account_api.account_positions(
                account_index=ACCOUNT_INDEX,
                authorization=auth_token,
                auth=auth_token,
            )
            for pos in (resp.positions or []):
                if int(pos.market_id) == MARKET_INDEX:
                    sign = 1 if int(pos.sign) >= 0 else -1
                    self.net_position    = float(pos.position) * sign
                    self.avg_entry_price = float(pos.avg_entry_price)
                    log.info(
                        f"📊 Position synced: {self.net_position:+.6f} base "
                        f"@ avg {self.avg_entry_price:.4f}"
                    )
                    break
        except Exception as e:
            log.warning(f"Position sync failed: {e}")

    # ── Main quoting loop ─────────────────────────────────────

    async def quoting_loop(self):
        mode = "DRY RUN" if self.dry_run else "LIVE"
        log.info(
            f"🚀 Quoting loop started  mode={mode}  "
            f"market={MARKET_INDEX}  account={ACCOUNT_INDEX}"
        )
        self.state = BotState.QUOTING

        while self._running:
            try:
                try:
                    await asyncio.wait_for(self._ob_update_event.wait(), timeout=1.0)
                    self._ob_update_event.clear()
                except asyncio.TimeoutError:
                    pass

                if not self._running:
                    break

                # Circuit breaker check
                if self.mid_price and self._check_circuit_breaker():
                    if self.state != BotState.HALTED:
                        self.state = BotState.HALTED
                        await self._cancel_all()
                    continue

                # Auto-resume after circuit breaker
                if self.state == BotState.HALTED:
                    recent = [p for t, p in self._price_history if time.time() - t < 5]
                    if len(recent) >= 2 and min(recent) > 0:
                        calm_bps = (max(recent) - min(recent)) / min(recent) * 10_000
                        if calm_bps < CIRCUIT_BPS * 0.5:
                            log.info("✅ Circuit breaker reset — resuming quoting")
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
        desired    = self._desired_quotes()
        spread_bps = self._spread_bps()
        tag        = "[DRY] " if self.dry_run else ""

        if desired is None:
            log.debug(
                f"{tag}No quote opportunity  "
                f"spread={spread_bps:.2f if spread_bps else 'n/a'}bps  "
                f"(need ≥ {MIN_SPREAD_BPS}bps)"
            )
            for rec in (self.bid_order, self.ask_order):
                if rec and rec.status in ("open", "partial"):
                    await self._cancel_order(rec)
            return

        bid_px, ask_px = desired
        log.debug(
            f"{tag}OB mid={self.mid_price:.4f}  spread={spread_bps:.2f}bps  "
            f"→ bid={bid_px:.4f}  ask={ask_px:.4f}  "
            f"pos={self.net_position:+.6f}  skew={self._inventory_skew():+.3f}"
        )

        await self._manage_side(self.bid_order, is_ask=False, desired_price=bid_px)
        await self._manage_side(self.ask_order, is_ask=True,  desired_price=ask_px)

    async def _manage_side(
        self,
        current_order: Optional[OrderRecord],
        is_ask: bool,
        desired_price: float,
    ):
        side_name = "ASK" if is_ask else "BID"
        size      = self._adjusted_size(is_ask)

        if size < 1e-9:
            return

        if not self._inventory_ok(is_ask):
            if current_order and current_order.status in ("open", "partial"):
                log.info(f"📉 Inventory limit — canceling {side_name}")
                await self._cancel_order(current_order)
            return

        # No active order → place fresh
        if current_order is None or current_order.status in ("filled", "canceled"):
            new_rec = await self._place_order(is_ask, desired_price, size)
            if new_rec:
                if is_ask:
                    self.ask_order = new_rec
                else:
                    self.bid_order = new_rec
            return

        # Active order — requote if price has drifted
        price_diff_bps = (
            abs(desired_price - current_order.price) / current_order.price * 10_000
            if current_order.price > 0 else 9999
        )
        if price_diff_bps >= PRICE_MOVE_THRESH_BPS:
            new_size   = size if current_order.filled_base == 0 else current_order.remaining_base
            can_modify = self.dry_run or (current_order.server_order_index is not None)

            if can_modify:
                success = await self._modify_order(current_order, desired_price, new_size)
                if not success:
                    # Fall back: cancel + place fresh
                    await self._cancel_order(current_order)
                    new_rec = await self._place_order(is_ask, desired_price, size)
                    if new_rec:
                        if is_ask:
                            self.ask_order = new_rec
                        else:
                            self.bid_order = new_rec
            else:
                log.debug(f"{side_name} modify skipped — order still in-flight")

    # ── Stats loop ────────────────────────────────────────────

    async def stats_loop(self):
        while self._running:
            await asyncio.sleep(30)
            tag = "[DRY] " if self.dry_run else ""
            bid_info = (
                "none" if not self.bid_order
                else f"{self.bid_order.price:.4f} ({self.bid_order.status})"
            )
            ask_info = (
                "none" if not self.ask_order
                else f"{self.ask_order.price:.4f} ({self.ask_order.status})"
            )
            log.info(f"{tag}📊 STATS | {self.stats.summary()}")
            log.info(
                f"   pos={self.net_position:+.6f} base "
                f"(≈{self.net_position * (self.mid_price or 0):+.2f} USD)  "
                f"state={self.state.value}  "
                f"bid={bid_info}  ask={ask_info}"
            )

    # ── Entry / exit ──────────────────────────────────────────

    async def run(self):
        await self._sync_position_from_api()
        tasks = [
            asyncio.create_task(self.quoting_loop(), name="quoting"),
            asyncio.create_task(self.stats_loop(),   name="stats"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self):
        tag = "[DRY] " if self.dry_run else ""
        log.info(f"{tag}Shutting down gracefully...")
        self._running = False
        self.state    = BotState.SHUTDOWN
        await self._cancel_all()
        log.info(f"Final stats: {self.stats.summary()}")


# ─────────────────────────────────────────────
#  API KEY LOADER
# ─────────────────────────────────────────────

def load_api_keys() -> Dict[int, str]:
    """
    Collect API keys from env vars of the form:
        LIGHTER_API_KEY_0=<hex>
        LIGHTER_API_KEY_1=<hex>
    (.env is loaded at module import time, so both .env and real env work.)
    """
    keys: Dict[int, str] = {}
    for k, v in os.environ.items():
        if k.startswith("LIGHTER_API_KEY_"):
            suffix = k[len("LIGHTER_API_KEY_"):]
            try:
                keys[int(suffix)] = v.strip()
            except ValueError:
                pass
    return keys


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────

async def main():
    global ACCOUNT_INDEX, API_KEYS, BASE_URL, WS_URL

    # Re-read in case .env overrode module-level defaults
    BASE_URL      = os.getenv("LIGHTER_BASE_URL",      BASE_URL)
    WS_URL        = os.getenv("LIGHTER_WS_URL",        WS_URL)
    ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", str(ACCOUNT_INDEX)))
    API_KEYS      = load_api_keys()

    if not API_KEYS:
        raise RuntimeError(
            "No API keys found.\n"
            "Add LIGHTER_API_KEY_0=<hex_private_key> to your .env file or environment."
        )

    log.info(f"API keys loaded: {sorted(API_KEYS.keys())}  account={ACCOUNT_INDEX}")
    log.info(f"Market: {MARKET_INDEX}  URL: {BASE_URL}")
    log.info(f"Dry run: {DRY_RUN}")


    api_client = ApiClient(configuration=Configuration(host=BASE_URL))

    if DRY_RUN:
        client = None   # no signing / no tx sending needed
        log.info("[DRY] Skipping SignerClient and API key check")
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

    # WebSocket — used in both live and dry-run (we need live OB data either way)
    ws = WsClient(
        url=WS_URL,
        order_book_ids=[MARKET_INDEX],
        account_ids=[] if DRY_RUN else [ACCOUNT_INDEX],
        on_order_book_update=bot.on_order_book_update,
        on_account_update=bot.on_account_update,
    )

    loop           = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        log.info("Signal received — initiating graceful shutdown")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass   # Windows

    ws_thread = threading.Thread(target=ws.run, daemon=True, name="ws-thread")
    ws_thread.start()

    log.info("⏳ Waiting for first order book tick...")
    timeout_s = 30
    t_start   = time.time()
    while bot.mid_price is None:
        if time.time() - t_start > timeout_s:
            raise RuntimeError("Timed out waiting for WebSocket order book data")
        await asyncio.sleep(0.05)

    log.info(
        f"✅ Order book ready  "
        f"bid={bot.best_bid:.4f}  ask={bot.best_ask:.4f}  "
        f"mid={bot.mid_price:.4f}  spread={bot._spread_bps():.2f}bps"
    )

    bot_task      = asyncio.create_task(bot.run(),             name="bot")
    shutdown_task = asyncio.create_task(shutdown_event.wait(), name="shutdown")

    done, pending = await asyncio.wait(
        [bot_task, shutdown_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    await api_client.close()
    if client:
        await client.close()
    log.info("👋 Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
