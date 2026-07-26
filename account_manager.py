import asyncio
import random
from datetime import datetime
from typing import Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from loguru import logger

from _websockets.ws_token import KickPoints
from _websockets.ws_connect import KickWebSocket
from utils.kick_utility import KickUtility
from utils.get_points_amount import PointsAmount

if TYPE_CHECKING:
    from discord_webhook import DiscordWebhook


@dataclass
class StreamerState:
    name: str
    priority: int

    is_online: bool = False
    is_watching: bool = False
    points: int = 0
    last_points_update: Optional[datetime] = None
    stream_id: Optional[int] = None
    channel_id: Optional[int] = None

    ws_client: Optional[KickWebSocket] = None
    ws_task: Optional[asyncio.Task] = None
    points_task: Optional[asyncio.Task] = None

    last_error: Optional[str] = None
    error_count: int = 0
    consecutive_none: int = 0


@dataclass
class AccountState:
    alias: str
    token: str
    proxy: Optional[str]
    max_concurrent: int
    streamers: Dict[str, StreamerState] = field(default_factory=dict)
    streamer_order: List[str] = field(default_factory=list)

    @property
    def active_count(self) -> int:
        return sum(1 for s in self.streamers.values() if s.is_watching)

    @property
    def active_names(self) -> List[str]:
        return [s.name for s in self.streamers.values() if s.is_watching]


class AccountWorker:
    def __init__(
        self,
        account_cfg: dict,
        global_proxy: Optional[str] = None,
        check_interval: int = 120,
        reconnect_cooldown: int = 600,
        stagger_min: float = 3.0,
        stagger_max: float = 8.0,
    ):
        self.alias = account_cfg["alias"]
        self.token = account_cfg["token"]
        self.proxy = account_cfg.get("proxy") or global_proxy
        self.max_concurrent = account_cfg.get("max_concurrent", 2)
        self.check_interval = check_interval
        self.reconnect_cooldown = reconnect_cooldown
        self.stagger_min = stagger_min
        self.stagger_max = stagger_max

        streamer_names: List[str] = account_cfg.get("streamers", [])
        clean_names = []
        for n in streamer_names:
            if n and n.strip():
                clean_names.append(n.strip())
            else:
                logger.warning(
                    f"[{self.alias}] Ignoring blank streamer entry in config "
                    f"(check for trailing commas/empty strings)"
                )
        streamer_names = clean_names

        self.state = AccountState(
            alias=self.alias,
            token=self.token,
            proxy=self.proxy,
            max_concurrent=self.max_concurrent,
            streamer_order=streamer_names,
        )
        for idx, name in enumerate(streamer_names):
            self.state.streamers[name] = StreamerState(
                name=name, priority=idx
            )

        self._utility_cache: Dict[str, KickUtility] = {}
        self._points_checker: Optional[PointsAmount] = None
        self._ws_token_getter: Optional[KickPoints] = None
        self._discord: Optional["DiscordWebhook"] = None

        self._rebalance_lock = asyncio.Lock()
        self._running = False

    def set_discord(self, discord: "DiscordWebhook"):
        self._discord = discord

    def _get_utility(self, streamer: str) -> KickUtility:
        if streamer not in self._utility_cache:
            self._utility_cache[streamer] = KickUtility(
                streamer, proxy=self.proxy
            )
        return self._utility_cache[streamer]

    def _get_points_checker(self) -> PointsAmount:
        if self._points_checker is None:
            self._points_checker = PointsAmount(proxy=self.proxy)
        return self._points_checker

    def _get_ws_token_getter(self) -> KickPoints:
        if self._ws_token_getter is None:
            self._ws_token_getter = KickPoints(
                self.token, proxy=self.proxy
            )
        return self._ws_token_getter

    async def start(self):
        self._running = True
        logger.info(
            f"[{self.alias}] Starting: "
            f"{len(self.state.streamers)} streamers, "
            f"limit={self.max_concurrent}, "
            f"proxy={'yes' if self.proxy else 'no'}"
        )

        try:
            await self._check_all_online()
            await self._rebalance()

            while self._running:
                jitter = random.uniform(
                    self.check_interval * 0.8,
                    self.check_interval * 1.2,
                )
                await asyncio.sleep(jitter)
                await self._check_all_online()
                await self._rebalance()

        except asyncio.CancelledError:
            logger.info(f"[{self.alias}] Worker cancelled")
        except Exception as e:
            logger.error(f"[{self.alias}] Worker crashed: {e}")
        finally:
            await self.stop()

    async def _check_all_online(self):
        for name in self.state.streamer_order:
            if not self._running:
                break
            try:
                utility = self._get_utility(name)
                stream_id = utility.get_stream_id(self.token)

                st = self.state.streamers[name]
                was_online = st.is_online
                st.is_online = stream_id is not None
                st.stream_id = stream_id

                if not was_online and st.is_online:
                    logger.info(
                        f"[{self.alias}] 🟢 {name} "
                        f"ONLINE (stream={stream_id})"
                    )
                    if self._discord:
                        self._discord.send_streamer_online(
                            self.alias, name,
                            st.priority, "online"
                        )

                elif was_online and not st.is_online:
                    logger.info(
                        f"[{self.alias}] 🔴 {name} OFFLINE"
                    )
                    if self._discord:
                        self._discord.send_streamer_online(
                            self.alias, name,
                            st.priority, "offline"
                        )

                elif not st.is_online:
                    logger.debug(
                        f"[{self.alias}] ⚫ {name} offline "
                        f"(stream_id=None)"
                    )

                await asyncio.sleep(random.uniform(1.0, 2.5))

            except Exception as e:
                logger.warning(
                    f"[{self.alias}] Error checking {name}: {e}"
                )

    async def _rebalance(self):
        async with self._rebalance_lock:
            online_by_priority = [
                name
                for name in self.state.streamer_order
                if self.state.streamers[name].is_online
            ]

            desired = set(
                online_by_priority[: self.max_concurrent]
            )
            current = {
                name
                for name, s in self.state.streamers.items()
                if s.is_watching
            }

            to_stop = current - desired
            for name in to_stop:
                reason = (
                    "offline"
                    if not self.state.streamers[name].is_online
                    else "displaced by priority"
                )
                logger.info(
                    f"[{self.alias}] ⏹ {name} — {reason}"
                )
                await self._stop_streamer(name)

                if (
                    self._discord
                    and reason == "displaced by priority"
                ):
                    self._discord.send_streamer_online(
                        self.alias, name,
                        self.state.streamers[name].priority,
                        "displaced"
                    )

            to_start = desired - current
            for name in to_start:
                pri = self.state.streamers[name].priority
                logger.info(
                    f"[{self.alias}] ▶ {name} "
                    f"(priority={pri})"
                )
                await self._start_streamer(name)

                if self._discord:
                    self._discord.send_streamer_online(
                        self.alias, name, pri, "started"
                    )

                await asyncio.sleep(
                    random.uniform(
                        self.stagger_min, self.stagger_max
                    )
                )

            if desired:
                logger.info(
                    f"[{self.alias}] Active: "
                    f"{sorted(desired)} "
                    f"({len(desired)}/{self.max_concurrent})"
                )


    async def _start_streamer(self, name: str):
        st = self.state.streamers[name]

        try:
            if not st.channel_id:
                utility = self._get_utility(name)
                st.channel_id = utility.get_channel_id(
                    self.token
                )
                if not st.channel_id:
                    raise RuntimeError(
                        f"Failed to get channel_id "
                        f"for {name}"
                    )

            ws_token_getter = self._get_ws_token_getter()
            ws_token = ws_token_getter.get_ws_token(name)
            if not ws_token:
                raise RuntimeError(
                    f"Failed to get WS token for {name}"
                )

            async def on_disconnect():
                st.is_watching = False
                logger.warning(
                    f"[{self.alias}] WS {name} "
                    f"disconnected permanently"
                )

            ws_client = KickWebSocket(
                data={
                    "token": ws_token,
                    "streamId": st.stream_id or 0,
                    "channelId": st.channel_id,
                },
                proxy=self.proxy,
                on_disconnect=on_disconnect,
            )

            st.ws_client = ws_client
            st.is_watching = True
            st.error_count = 0

            st.ws_task = asyncio.create_task(
                self._ws_wrapper(name, ws_client)
            )
            st.points_task = asyncio.create_task(
                self._points_loop(name)
            )

            try:
                pts = self._get_points_checker().get_amount(
                    name, self.token
                )
                if pts is not None:
                    st.points = pts
                    st.last_points_update = datetime.now()
            except Exception as e:
                logger.debug(
                    f"[{self.alias}] Initial points fetch for {name} "
                    f"failed (will retry on next cycle): {e}"
                )

            logger.success(
                f"[{self.alias}] ✅ Watching {name}"
            )

        except Exception as e:
            logger.error(
                f"[{self.alias}] Error starting {name}: {e}"
            )
            st.error_count += 1
            st.last_error = str(e)
            st.is_watching = False

            if self._discord:
                self._discord.send_error(
                    self.alias, name, str(e)
                )

    async def _stop_streamer(self, name: str):
        st = self.state.streamers[name]

        for task in (st.ws_task, st.points_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if st.ws_client:
            try:
                await st.ws_client.disconnect()
            except Exception:
                pass

        st.is_watching = False
        st.ws_client = None
        st.ws_task = None
        st.points_task = None
        logger.info(f"[{self.alias}] ⏹ {name} stopped")

    async def _ws_wrapper(
        self, name: str, ws_client: KickWebSocket
    ):
        try:
            await ws_client.connect()
        except Exception as e:
            logger.error(
                f"[{self.alias}] WS {name} crashed: {e}"
            )
        finally:
            self.state.streamers[name].is_watching = False

    async def _points_loop(self, name: str):
        st = self.state.streamers[name]
        checker = self._get_points_checker()

        while st.is_watching and self._running:
            try:
                await asyncio.sleep(random.uniform(120, 180))
                if not st.is_watching:
                    break

                amount = checker.get_amount(name, self.token)
                if amount is None:
                    st.consecutive_none += 1
                    if st.consecutive_none == 3:
                        logger.warning(
                            f"[{self.alias}] {name}: points check has "
                            f"failed 3 cycles in a row — check token/proxy "
                            f"if this continues"
                        )
                    else:
                        logger.debug(
                            f"[{self.alias}] Points check for {name} returned "
                            f"nothing this cycle (rate limit / proxy / token?)"
                        )
                    continue

                st.consecutive_none = 0

                old = st.points
                st.points = amount
                st.last_points_update = datetime.now()

                if amount > old:
                    gain = amount - old
                    logger.success(
                        f"[{self.alias}] 💰 {name}: "
                        f"+{gain} (Total: {amount})"
                    )

                    if self._discord:
                        self._discord.send_points_update(
                            self.alias, name, old, amount
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"[{self.alias}] Points error "
                    f"{name}: {e}"
                )

    async def stop(self):
        self._running = False

        for name in list(self.state.streamers):
            if self.state.streamers[name].is_watching:
                await self._stop_streamer(name)

        if self._points_checker:
            self._points_checker.close()
            self._points_checker = None

        if self._ws_token_getter:
            self._ws_token_getter.close()
            self._ws_token_getter = None

        for u in self._utility_cache.values():
            u.close()
        self._utility_cache.clear()

        logger.info(f"[{self.alias}] Worker stopped")

    def get_status(self) -> dict:
        return {
            "alias": self.alias,
            "proxy": bool(self.proxy),
            "max_concurrent": self.max_concurrent,
            "active_count": self.state.active_count,
            "active_streamers": self.state.active_names,
            "streamer_order": self.state.streamer_order,
            "streamers": {
                name: {
                    "priority": s.priority,
                    "online": s.is_online,
                    "watching": s.is_watching,
                    "points": s.points,
                    "last_update": (
                        s.last_points_update.isoformat()
                        if s.last_points_update
                        else None
                    ),
                    "stream_id": s.stream_id,
                    "errors": s.error_count,
                }
                for name, s in self.state.streamers.items()
            },
        }


class AccountManager:
    def __init__(self, config: dict):
        self.workers: List[AccountWorker] = []
        self._tasks: List[asyncio.Task] = []
        self._discord: Optional["DiscordWebhook"] = None

        proxy_cfg = config.get("Proxy", {})
        global_proxy = (
            proxy_cfg.get("url")
            if proxy_cfg.get("enabled") else None
        )

        check_interval = config.get("Check_interval", 120)
        reconnect_cooldown = config.get(
            "Reconnect_cooldown", 600
        )
        stagger_min = config.get("Connection_stagger_min", 3)
        stagger_max = config.get("Connection_stagger_max", 8)

        # Backward compatibility
        accounts = config.get("Accounts", [])
        if not accounts:
            old_token = config.get(
                "Private", {}
            ).get("token", "")
            old_streamers = config.get("Streamers", [])
            old_max = config.get("Max_active_channels", 5)
            if old_token and old_streamers:
                accounts = [{
                    "alias": "Default",
                    "token": old_token,
                    "streamers": old_streamers,
                    "max_concurrent": old_max,
                }]
                logger.warning(
                    "⚠️ Legacy config format detected. "
                    "Please migrate to Accounts[]."
                )

        for acc in accounts:
            self.workers.append(
                AccountWorker(
                    acc,
                    global_proxy=global_proxy,
                    check_interval=check_interval,
                    reconnect_cooldown=reconnect_cooldown,
                    stagger_min=stagger_min,
                    stagger_max=stagger_max,
                )
            )

        logger.info(
            f"📊 Accounts loaded: {len(self.workers)}, "
            f"global proxy: "
            f"{'yes' if global_proxy else 'no'}"
        )

    def set_discord(self, discord: "DiscordWebhook"):
        """Attach the Discord webhook to all accounts"""
        self._discord = discord
        for worker in self.workers:
            worker.set_discord(discord)
        logger.info(
            f"🟣 Discord webhook attached to "
            f"{len(self.workers)} accounts"
        )

    async def start_all(self):
        for i, worker in enumerate(self.workers):
            if i > 0:
                delay = random.uniform(5, 15)
                logger.info(
                    f"⏳ Delaying {delay:.0f}s before "
                    f"account [{worker.alias}]"
                )
                await asyncio.sleep(delay)

            task = asyncio.create_task(worker.start())
            self._tasks.append(task)

        await asyncio.gather(
            *self._tasks, return_exceptions=True
        )

    async def stop_all(self):
        for task in self._tasks:
            task.cancel()
        for w in self.workers:
            await w.stop()

    def get_all_status(self) -> List[dict]:
        return [w.get_status() for w in self.workers]

    def get_all_streamers_flat(self) -> List[str]:
        seen = set()
        result = []
        for w in self.workers:
            for s in w.state.streamer_order:
                if s not in seen:
                    seen.add(s)
                    result.append(s)
        return result