import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..logger import get_logger
from ..config import Config
from ..utils import has_chinese, utc_to_utc8_datetime
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class StarhubPlatform(BaseEPGPlatform):
    """StarHub EPG platform implementation"""

    def __init__(self):
        super().__init__("starhub")
        self.base_url = "https://waf-starhub-metadata-api-p001.ifs.vubiquity.com/v3.1/epg"
        self.channels_url = f"{self.base_url}/channels"
        self.schedules_url = f"{self.base_url}/schedules"

    async def fetch_channels(self) -> List[Channel]:
        """Fetch channel list from StarHub API"""
        self.logger.info("📡 正在从 StarHub 获取频道列表")

        headers = self.get_default_headers()

        params = {
            "locale": "zh",
            "locale_default": "en_US",
            "device": "1",
            "limit": "150",
            "page": "1"
        }

        response = self.http_client.get(
            self.channels_url,
            headers=headers,
            params=params
        )

        data = response.json()
        channels = []

        for resource in data.get('resources', []):
            if resource.get('metatype') == 'Channel':
                channels.append(Channel(
                    channel_id=resource.get('id', ''),
                    name=resource.get('title', ''),
                    raw_data=resource
                ))

        self.logger.info(f"📺 从 StarHub 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch program data for all StarHub channels with concurrency limit"""
        concurrency = 10
        self.logger.info(f"📡 正在抓取 {len(channels)} 个 StarHub 频道的节目数据 (并发数: {concurrency})")

        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(channel: Channel, session: aiohttp.ClientSession):
            async with semaphore:
                return await self._fetch_channel_programs(channel, session)

        timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)
        headers = self.get_default_headers()

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            tasks = [fetch_with_semaphore(channel, session) for channel in channels]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_programs = []
        error_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_count += 1
                self.logger.error(f"❌ 获取频道 {channels[i].name} 节目数据失败: {result}")
            else:
                all_programs.extend(result)

        self.logger.info(f"📊 总共抓取了 {len(all_programs)} 个节目 (成功: {len(channels) - error_count}, 失败: {error_count})")
        return all_programs

    @retry(
        stop=stop_after_attempt(Config.HTTP_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.HTTP_RETRY_BACKOFF),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def _fetch_channel_programs(self, channel: Channel, session: aiohttp.ClientSession) -> List[Program]:
        """Fetch program data for a specific StarHub channel (async)"""
        self.logger.info(f"🔍【Starhub】 正在获取频道节目: {channel.name} (ID: {channel.channel_id})")

        # Calculate time range (today to 6 days later)
        tz = ZoneInfo('Asia/Shanghai')
        today_start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
        six_days_later = today_start + timedelta(days=6)
        six_days_later_end = six_days_later.replace(hour=23, minute=59, second=59)

        today_timestamp = int(today_start.timestamp())
        six_days_later_timestamp = int(six_days_later_end.timestamp())

        params = {
            "locale": "zh",
            "locale_default": "en_US",
            "device": "1",
            "limit": "500",
            "page": "1",
            "in_channel_id": channel.channel_id,
            "gt_end": str(today_timestamp),      # Start time
            "lt_start": str(six_days_later_timestamp),  # End time
        }

        async with session.get(self.schedules_url, params=params) as response:
            data = await response.json()

        programs = []

        for resource in data.get('resources', []):
            if resource.get('metatype') == 'Schedule':
                try:
                    title = resource.get('title', '')
                    description = resource.get('description', '')
                    episode_number = resource.get('episode_number')

                    # Add episode number based on language
                    if episode_number:
                        if has_chinese(title) or has_chinese(description):
                            title += f" 第{episode_number}集"
                        else:
                            title += f" Ep{episode_number}"

                    # Convert timestamps to datetime objects
                    start_time = utc_to_utc8_datetime(resource.get('start'))
                    end_time = utc_to_utc8_datetime(resource.get('end'))

                    programs.append(Program(
                        channel_id=channel.channel_id,
                        title=title,
                        start_time=start_time,
                        end_time=end_time,
                        description=description,
                        raw_data=resource
                    ))

                except Exception as e:
                    self.logger.warning(f"⚠️ 解析节目数据失败: {e}")
                    continue

        self.logger.debug(f"📺 在 {channel.name} 中发现 {len(programs)} 个节目")
        return programs


# Create platform instance
starhub_platform = StarhubPlatform()


# Legacy functions for backward compatibility
def request_channels():
    """Legacy function - get StarHub channel list (synchronous)"""
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("⚠️ 在异步上下文中调用旧版 request_channels - 返回空列表")
            return []
        else:
            channels = loop.run_until_complete(starhub_platform.fetch_channels())
            # Convert to legacy format
            return [{"channelName": ch.name, "channelId": ch.channel_id} for ch in channels]
    except Exception as e:
        logger.error(f"❌ 旧版 request_channels 错误: {e}")
        return []


def request_epg(channel_id, channel_name):
    """Legacy function - get EPG for specific channel (synchronous)"""
    try:
        import asyncio

        async def _fetch():
            channel = Channel(channel_id=channel_id, name=channel_name)
            timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)
            headers = starhub_platform.get_default_headers()

            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                programs = await starhub_platform._fetch_channel_programs(channel, session)

            # Convert to legacy format
            return [{
                "channelName": channel_name,
                "programName": p.title,
                "description": p.description,
                "start": p.start_time,
                "end": p.end_time
            } for p in programs]

        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("⚠️ 在异步上下文中调用旧版 request_epg - 返回空列表")
            return []
        else:
            return loop.run_until_complete(_fetch())
    except Exception as e:
        logger.error(f"❌ 旧版 request_epg 错误: {e}")
        return []


async def get_starhub_epg():
    """Legacy function - fetch StarHub EPG data"""
    try:
        channels = await starhub_platform.fetch_channels()
        programs = await starhub_platform.fetch_programs(channels)

        # Convert to legacy format
        raw_channels = []
        raw_programs = []

        for channel in channels:
            raw_channels.append({
                "channelName": channel.name,
                "channelId": channel.channel_id
            })

        for program in programs:
            channel_name = next((ch.name for ch in channels if ch.channel_id == program.channel_id), "")
            raw_programs.append({
                "channelName": channel_name,
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time
            })

        return raw_channels, raw_programs

    except Exception as e:
        logger.error(f"❌ 旧版 get_starhub_epg 函数错误: {e}", exc_info=True)
        return [], []