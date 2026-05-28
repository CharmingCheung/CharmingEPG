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


def _build_aiohttp_connector(proxy_url: Optional[str]):
    """Build an aiohttp connector. Returns (connector, http_proxy_url).
    If proxy is socks*, returns a ProxyConnector and http_proxy_url=None.
    If proxy is http/https, returns (None, proxy_url) so aiohttp uses request-level proxy.
    """
    if not proxy_url:
        return None, None

    lowered = proxy_url.lower()
    if lowered.startswith(("socks4://", "socks4a://", "socks5://", "socks5h://")):
        try:
            from aiohttp_socks import ProxyConnector
        except ImportError as e:
            raise RuntimeError(
                "SOCKS proxy configured for Singtel but aiohttp-socks is not installed. "
                "Run: pip install aiohttp-socks"
            ) from e
        return ProxyConnector.from_url(proxy_url), None

    # http(s) proxy
    return None, proxy_url


class SingtelPlatform(BaseEPGPlatform):
    """Singtel CAST EPG platform implementation for Singapore"""

    API_KEY = "weLnqiyqPWw6zQuVf9tXbpssrL2VVDTbzHiVbSnw"
    BASE_URL = "https://api.v3.singtelcast.com/v1"

    # Singtel only accepts these fixed time slots (UTC)
    TIME_SLOTS = [
        ("T16:00:00.000Z", "T21:59:59.999Z"),
        ("T22:00:00.000Z", "T03:59:59.999Z"),  # crosses to next day
        ("T04:00:00.000Z", "T09:59:59.999Z"),
        ("T10:00:00.000Z", "T15:59:59.999Z"),
    ]

    def __init__(self):
        super().__init__("singtel")
        self.channels_url = f"{self.BASE_URL}/channels/"
        self.epg_url = f"{self.BASE_URL}/channels/epg/"

    def get_singtel_headers(self) -> dict:
        """Get headers required by Singtel API"""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "no-cache",
            "origin": "https://watchcast.singtel.com",
            "pragma": "no-cache",
            "referer": "https://watchcast.singtel.com/",
            "user-agent": Config.DEFAULT_USER_AGENT,
            "x-api-key": self.API_KEY,
        }

    async def fetch_channels(self) -> List[Channel]:
        """Fetch all free channels from Singtel API"""
        self.logger.info("📡 正在从 Singtel 获取频道列表")

        headers = self.get_singtel_headers()
        params = {"offset": "0", "limit": "200"}

        proxy_url = Config.get_singtel_proxy()
        if proxy_url:
            self.logger.info(f"🔌 Singtel 使用代理: {self._mask_proxy(proxy_url)}")

        # Use requests directly so we can apply Singtel-specific proxy
        # (socks support requires `requests[socks]` extra)
        import requests
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        response = requests.get(
            self.channels_url,
            headers=headers,
            params=params,
            proxies=proxies,
            timeout=Config.HTTP_TIMEOUT,
        )
        response.raise_for_status()

        data = response.json()
        channels = []

        for item in data.get('data', []):
            epg_channel_id = item.get('epgChannelId')
            title = item.get('title')
            if not epg_channel_id or not title:
                continue
            channels.append(Channel(
                channel_id=str(epg_channel_id),
                name=title,
                raw_data=item
            ))

        self.logger.info(f"📺 从 Singtel 发现 {len(channels)} 个频道")
        return channels

    @staticmethod
    def _mask_proxy(proxy_url: str) -> str:
        """Mask credentials in proxy URL for logging"""
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(proxy_url)
            if parsed.username or parsed.password:
                netloc = f"***:***@{parsed.hostname}"
                if parsed.port:
                    netloc += f":{parsed.port}"
                return urlunparse(parsed._replace(netloc=netloc))
            return proxy_url
        except Exception:
            return "***"

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch EPG data for all Singtel channels in fixed 6-hour windows"""
        # Build a set of valid epg channel IDs for filtering
        valid_channel_ids = {ch.channel_id for ch in channels}

        # Build all (startdate, enddate) request windows for the next ~7 days
        # Singtel's day boundary starts at UTC 16:00 (which is 00:00 Asia/Shanghai)
        tz = ZoneInfo('Asia/Shanghai')
        today_local = datetime.now(tz).date()

        windows = []
        # Fetch from yesterday (covers the early hours of today local time)
        # through 6 days ahead.
        for day_offset in range(-1, 7):
            base_date = today_local + timedelta(days=day_offset)
            for start_suffix, end_suffix in self.TIME_SLOTS:
                start_date_str = base_date.strftime('%Y-%m-%d')
                # If end slot is "earlier" than start (slot crossing midnight UTC),
                # then end date is next day
                if end_suffix < start_suffix:
                    end_date = base_date + timedelta(days=1)
                else:
                    end_date = base_date
                end_date_str = end_date.strftime('%Y-%m-%d')

                start = f"{start_date_str}{start_suffix}"
                end = f"{end_date_str}{end_suffix}"
                windows.append((start, end))

        concurrency = 5
        self.logger.info(f"📡 正在抓取 Singtel EPG 数据 (窗口数: {len(windows)}, 并发数: {concurrency})")

        semaphore = asyncio.Semaphore(concurrency)

        proxy_url = Config.get_singtel_proxy()
        connector, http_proxy = _build_aiohttp_connector(proxy_url)

        async def fetch_with_semaphore(start, end, session):
            async with semaphore:
                return await self._fetch_window(start, end, session, http_proxy)

        timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)
        headers = self.get_singtel_headers()

        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=headers,
            connector=connector,
        ) as session:
            tasks = [fetch_with_semaphore(s, e, session) for s, e in windows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_programs = []
        error_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_count += 1
                s, e = windows[i]
                self.logger.error(f"❌ 获取 Singtel EPG 窗口 {s} ~ {e} 失败: {result}")
            else:
                # Filter to only programs whose channel is in our channel list
                for prog in result:
                    if prog.channel_id in valid_channel_ids:
                        all_programs.append(prog)

        self.logger.info(
            f"📊 总共抓取了 {len(all_programs)} 个 Singtel 节目 "
            f"(成功: {len(windows) - error_count}, 失败: {error_count})"
        )
        return all_programs

    @retry(
        stop=stop_after_attempt(Config.HTTP_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.HTTP_RETRY_BACKOFF),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def _fetch_window(self, start_date: str, end_date: str, session: aiohttp.ClientSession,
                            http_proxy: Optional[str] = None) -> List[Program]:
        """Fetch a single 6-hour EPG window for all channels"""
        self.logger.debug(f"🔍【Singtel】 获取窗口: {start_date} ~ {end_date}")

        params = {
            "offset": "0",
            "limit": "10000",
            "startdate": start_date,
            "enddate": end_date,
        }

        request_kwargs = {"params": params}
        if http_proxy:
            request_kwargs["proxy"] = http_proxy

        async with session.get(self.epg_url, **request_kwargs) as response:
            data = await response.json()

        programs = []
        tz_shanghai = ZoneInfo('Asia/Shanghai')

        for item in data.get('data', []):
            try:
                channel_id = str(item.get('epgChannelId', ''))
                if not channel_id:
                    continue

                title = item.get('title', '') or ''
                subtitle = item.get('subtitle', '') or ''
                description = item.get('description', '') or ''

                if subtitle:
                    title = f"{title} {subtitle}".strip()

                start_str = item.get('startDate')
                duration = item.get('duration')
                if not start_str or duration is None:
                    continue

                # startDate is naive ISO without timezone, but represents UTC
                start_utc = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
                end_utc = start_utc + timedelta(seconds=int(duration))

                start_time = start_utc.astimezone(tz_shanghai)
                end_time = end_utc.astimezone(tz_shanghai)

                programs.append(Program(
                    channel_id=channel_id,
                    title=title,
                    start_time=start_time,
                    end_time=end_time,
                    description=description,
                    raw_data=item
                ))

            except Exception as e:
                self.logger.warning(f"⚠️ 解析 Singtel 节目数据失败: {e}")
                continue

        return programs


# Create platform instance
singtel_platform = SingtelPlatform()


# Legacy function for backward compatibility / main.py integration
async def get_singtel_epg():
    """Fetch Singtel EPG data and return (channels, programs) in legacy dict format"""
    try:
        channels = await singtel_platform.fetch_channels()
        programs = await singtel_platform.fetch_programs(channels)

        raw_channels = []
        raw_programs = []

        for channel in channels:
            raw_channels.append({
                "channelName": channel.name,
                "channelId": channel.channel_id
            })

        for program in programs:
            channel_name = next(
                (ch.name for ch in channels if ch.channel_id == program.channel_id),
                ""
            )
            raw_programs.append({
                "channelName": channel_name,
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time
            })

        return raw_channels, raw_programs

    except Exception as e:
        logger.error(f"❌ get_singtel_epg 函数错误: {e}", exc_info=True)
        return [], []
