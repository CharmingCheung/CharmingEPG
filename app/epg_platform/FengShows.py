import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..logger import get_logger
from ..config import Config
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class FengShowsPlatform(BaseEPGPlatform):
    """FengShows (凤凰秀) EPG platform implementation.

    抓取凤凰卫视三个频道的节目表（凤凰资讯台 / 凤凰中文台 / 凤凰香港台）。
    API 按天返回节目列表，仅有开始时间（UTC），结束时间由相邻节目推算。
    """

    API_BASE = "https://api.fengshows.cn"
    REFERER = "https://www.fengshows.com/"

    # Number of days to fetch, starting from today (Asia/Shanghai)
    DAYS_AHEAD = 7

    # (live_id, channel_name)
    CHANNELS = [
        ("7c96b084-60e1-40a9-89c5-682b994fb680", "凤凰资讯台"),
        ("f7f48462-9b13-485b-8101-7b54716411ec", "凤凰中文台"),
        ("15e02d92-1698-416c-af2f-3e9a872b4d78", "凤凰香港台"),
    ]

    def __init__(self):
        super().__init__("fengshows")

    def get_fengshows_headers(self) -> dict:
        """Get headers for FengShows API requests (the custom client header is required)"""
        return self.get_default_headers({
            "fengshows-client": "app(fs-web,1000000);",
            "origin": "https://www.fengshows.com",
            "referer": self.REFERER,
        })

    async def fetch_channels(self) -> List[Channel]:
        """Return the fixed FengShows channel list"""
        self.logger.info("📡 正在从 FengShows 获取频道列表")
        channels = [
            Channel(channel_id=name, name=name, live_id=live_id)
            for live_id, name in self.CHANNELS
        ]
        self.logger.info(f"📺 从 FengShows 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch EPG data for all channels, one request per channel per local day"""
        tz = ZoneInfo('Asia/Shanghai')
        today_local = datetime.now(tz).date()
        dates = [(today_local + timedelta(days=off)).strftime("%Y%m%d")
                 for off in range(self.DAYS_AHEAD)]

        concurrency = 3
        self.logger.info(
            f"📡 正在抓取 FengShows EPG 数据 "
            f"(频道: {len(channels)}, 天数: {len(dates)}, 并发数: {concurrency})"
        )

        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(channel: Channel, date_str: str, session):
            async with semaphore:
                items = await self._fetch_schedule(
                    channel.extra_data["live_id"], date_str, session
                )
                return channel.channel_id, items

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT),
            headers=self.get_fengshows_headers(),
        ) as session:
            tasks = [fetch_one(ch, d, session) for ch in channels for d in dates]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect raw (start, title, desc) entries per channel before deriving stop times
        raw_by_channel = {ch.channel_id: [] for ch in channels}
        error_count = 0
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
                self.logger.error(f"❌ 获取 FengShows EPG 失败: {result}")
                continue
            channel_id, items = result
            for item in items:
                entry = self._extract_entry(item)
                if entry:
                    raw_by_channel[channel_id].append(entry)

        all_programs = []
        for channel_id, entries in raw_by_channel.items():
            all_programs.extend(self._fill_stop_times(channel_id, entries))

        self.logger.info(
            f"📊 总共抓取了 {len(all_programs)} 个 FengShows 节目 "
            f"(失败请求: {error_count})"
        )
        return all_programs

    @retry(
        stop=stop_after_attempt(Config.HTTP_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.HTTP_RETRY_BACKOFF),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def _fetch_schedule(self, live_id: str, date_str: str,
                              session: aiohttp.ClientSession) -> List[dict]:
        """Fetch one channel's schedule for a single day (date_str: YYYYMMDD)"""
        self.logger.debug(f"🔍【FengShows】 获取 live_id={live_id} date={date_str}")

        url = f"{self.API_BASE}/live/{live_id}/resources"
        params = {"dir": "asc", "date": date_str, "page": "1", "page_size": "99"}

        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

        # The API may return a bare list or a wrapped object
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, list):
                return inner
            if isinstance(inner, dict):
                items = inner.get("items", inner.get("list", inner.get("resources", [])))
                if isinstance(items, list):
                    return items
        return []

    def _extract_entry(self, item: dict) -> Optional[dict]:
        """Extract (title, start, desc) from a single API record"""
        title = (item.get("title") or "").strip()
        if not title:
            return None

        start = self._parse_time(item.get("event_time", ""))
        if start is None:
            return None

        desc = (item.get("brief") or item.get("content") or "").strip()
        return {"title": title, "start": start, "desc": desc}

    @staticmethod
    def _parse_time(ts_str: str) -> Optional[datetime]:
        """Parse the API time string into a timezone-aware datetime.

        Strings ending with "Z" are UTC; others are assumed to be Asia/Shanghai.
        EpgGenerator handles the final conversion to Asia/Shanghai.
        """
        if not ts_str:
            return None
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
        ):
            try:
                dt = datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
            if ts_str.endswith("Z"):
                return dt.replace(tzinfo=timezone.utc)
            return dt.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
        return None

    def _fill_stop_times(self, channel_id: str, entries: List[dict]) -> List[Program]:
        """Sort a channel's entries and use the next start as the current stop time.

        The last program of the channel defaults to a 1-hour duration.
        """
        entries.sort(key=lambda e: e["start"])
        programs = []
        for i, entry in enumerate(entries):
            if i + 1 < len(entries):
                end_time = entries[i + 1]["start"]
            else:
                end_time = entry["start"] + timedelta(hours=1)
            programs.append(Program(
                channel_id=channel_id,
                title=entry["title"],
                start_time=entry["start"],
                end_time=end_time,
                description=entry["desc"],
            ))
        return programs


# Create platform instance
fengshows_platform = FengShowsPlatform()


# Legacy function for backward compatibility / main.py integration
async def get_fengshows_epg():
    """Fetch FengShows EPG data and return (channels, programs) in legacy dict format"""
    try:
        channels = await fengshows_platform.fetch_channels()
        programs = await fengshows_platform.fetch_programs(channels)

        raw_channels = [
            {"channelName": channel.name, "channelId": channel.channel_id}
            for channel in channels
        ]

        raw_programs = [
            {
                "channelName": program.channel_id,
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time,
            }
            for program in programs
        ]

        return raw_channels, raw_programs

    except Exception as e:
        logger.error(f"❌ get_fengshows_epg 函数错误: {e}", exc_info=True)
        return [], []
