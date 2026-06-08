import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..logger import get_logger
from ..config import Config
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


def _pick_localized(entries: List[dict], prefer: str = "zh") -> str:
    """Extract a localized string from UnifiTV's [{"lang": ..., "n": ...}] lists.

    Prefers the requested language, falls back to English, then the first entry.
    The "n" value may be a string or a list of strings (e.g. genres).
    """
    if not entries:
        return ""

    def value_of(entry: dict) -> str:
        n = entry.get("n", "")
        if isinstance(n, list):
            return " ".join(str(x) for x in n)
        return str(n)

    by_lang = {e.get("lang"): e for e in entries if isinstance(e, dict)}
    for lang in (prefer, "en"):
        if lang in by_lang:
            return value_of(by_lang[lang])
    return value_of(entries[0]) if isinstance(entries[0], dict) else ""


class UnifiTVPlatform(BaseEPGPlatform):
    """UnifiTV (TM unifi, Malaysia) EPG platform implementation"""

    BASE_URL = "https://data-store-cdn.api.tmcms.quickplay.com/content/epg"

    # Number of days to fetch, starting from today (Asia/Shanghai)
    DAYS_AHEAD = 7

    def __init__(self):
        super().__init__("unifitv")

    def get_unifitv_headers(self) -> dict:
        """Get headers for UnifiTV API requests"""
        return self.get_default_headers({
            "origin": "https://unifitv.com.my",
            "referer": "https://unifitv.com.my/",
        })

    async def fetch_channels(self) -> List[Channel]:
        """Fetch the channel list (channels are returned alongside the EPG).

        The UnifiTV EPG endpoint returns one entry per channel with its airings
        embedded, so we derive channels from a single day's request.
        """
        self.logger.info("📡 正在从 UnifiTV 获取频道列表")

        tz = ZoneInfo('Asia/Shanghai')
        today_local = datetime.now(tz).date()
        start, end = self._day_window(today_local)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT),
            headers=self.get_unifitv_headers(),
        ) as session:
            data = await self._fetch_day(start, end, session)

        channels = []
        for el in data:
            cs = el.get("cs")
            name = _pick_localized(el.get("lon", [])) or _pick_localized(el.get("lodn", []))
            if not cs or not name:
                continue
            channels.append(Channel(
                channel_id=name,
                name=name,
                cs=cs,
                cid=el.get("cid"),
            ))

        self.logger.info(f"📺 从 UnifiTV 发现 {len(channels)} 个频道")
        return channels

    @staticmethod
    def _day_window(local_date) -> tuple:
        """Build the (start, end) UTC ISO strings for a single Asia/Shanghai day.

        Asia/Shanghai 00:00 == previous-day 16:00 UTC.
        """
        tz = ZoneInfo('Asia/Shanghai')
        start_local = datetime(local_date.year, local_date.month, local_date.day, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc)
        end_utc = end_local.astimezone(timezone.utc)
        start = start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        end = end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        return start, end

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch EPG data for all channels, one request per local day"""
        tz = ZoneInfo('Asia/Shanghai')
        today_local = datetime.now(tz).date()

        windows = [self._day_window(today_local + timedelta(days=off))
                   for off in range(self.DAYS_AHEAD)]

        concurrency = 5
        self.logger.info(f"📡 正在抓取 UnifiTV EPG 数据 (天数: {len(windows)}, 并发数: {concurrency})")

        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(start, end, session):
            async with semaphore:
                data = await self._fetch_day(start, end, session)
                return self._parse_programs(data)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT),
            headers=self.get_unifitv_headers(),
        ) as session:
            tasks = [fetch_with_semaphore(s, e, session) for s, e in windows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_programs = []
        error_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_count += 1
                s, e = windows[i]
                self.logger.error(f"❌ 获取 UnifiTV EPG 窗口 {s} ~ {e} 失败: {result}")
            else:
                all_programs.extend(result)

        self.logger.info(
            f"📊 总共抓取了 {len(all_programs)} 个 UnifiTV 节目 "
            f"(成功: {len(windows) - error_count}, 失败: {error_count})"
        )
        return all_programs

    @retry(
        stop=stop_after_attempt(Config.HTTP_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.HTTP_RETRY_BACKOFF),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def _fetch_day(self, start: str, end: str, session: aiohttp.ClientSession) -> List[dict]:
        """Fetch one day's EPG payload (all channels) and return the data list"""
        self.logger.debug(f"🔍【UnifiTV】 获取窗口: {start} ~ {end}")

        params = {
            "start": start,
            "end": end,
            "reg": "my",
            "dt": "web",
            "client": "tm-unifitv-web",
            "locale": "zh",
            "pageSize": "100",
            "pageNumber": "1",
        }

        async with session.get(self.BASE_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()

        return data.get("data", [])

    def _parse_programs(self, data: List[dict]) -> List[Program]:
        """Parse the day payload into Program objects"""
        tz_shanghai = ZoneInfo('Asia/Shanghai')
        programs = []

        for el in data:
            channel_name = _pick_localized(el.get("lon", [])) or _pick_localized(el.get("lodn", []))
            if not channel_name:
                continue

            for airing in el.get("airing", []):
                try:
                    pgm = airing.get("pgm", {})
                    title = _pick_localized(pgm.get("lon", [])) or _pick_localized(pgm.get("lostl", []))
                    if not title:
                        continue
                    description = _pick_localized(pgm.get("lod", [])) or _pick_localized(pgm.get("lold", []))

                    start_str = airing.get("sc_st_dt")
                    end_str = airing.get("sc_ed_dt")
                    if not start_str or not end_str:
                        continue

                    start_utc = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    end_utc = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

                    start_time = start_utc.astimezone(tz_shanghai)
                    end_time = end_utc.astimezone(tz_shanghai)

                    programs.append(Program(
                        channel_id=channel_name,
                        title=title,
                        start_time=start_time,
                        end_time=end_time,
                        description=description,
                        raw_data=airing
                    ))
                except Exception as e:
                    self.logger.warning(f"⚠️ 解析 UnifiTV 节目数据失败: {e}")
                    continue

        return programs


# Create platform instance
unifitv_platform = UnifiTVPlatform()


# Legacy function for backward compatibility / main.py integration
async def get_unifitv_epg():
    """Fetch UnifiTV EPG data and return (channels, programs) in legacy dict format"""
    try:
        channels = await unifitv_platform.fetch_channels()
        programs = await unifitv_platform.fetch_programs(channels)

        valid_names = {ch.name for ch in channels}

        raw_channels = []
        raw_programs = []

        for channel in channels:
            raw_channels.append({
                "channelName": channel.name,
                "channelId": channel.channel_id
            })

        for program in programs:
            if program.channel_id not in valid_names:
                continue
            raw_programs.append({
                "channelName": program.channel_id,
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time
            })

        return raw_channels, raw_programs

    except Exception as e:
        logger.error(f"❌ get_unifitv_epg 函数错误: {e}", exc_info=True)
        return [], []
