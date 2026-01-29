import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..logger import get_logger
from ..config import Config
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class MeWatchPlatform(BaseEPGPlatform):
    """MeWatch EPG platform implementation for Singapore"""

    # Fixed list ID for channel guide (geo-independent)
    CHANNEL_LIST_ID = "239614"

    def __init__(self):
        super().__init__("mewatch")
        self.base_url = "https://cdn.mewatch.sg/api"

    async def fetch_channels(self) -> List[Channel]:
        """Fetch channel list from MeWatch API with pagination support"""
        self.logger.info("📡 正在从 MeWatch 获取频道列表")

        list_id = self.CHANNEL_LIST_ID

        all_channels = []
        page = 1
        page_size = 100

        while True:
            channels_url = f"{self.base_url}/lists/{list_id}"
            params = {
                "ff": "idp,ldp,rpt,cd",
                "lang": "zh",
                "page": str(page),
                "page_size": str(page_size),
                "segments": "all"
            }

            headers = self.get_default_headers()

            response = self.http_client.get(
                channels_url,
                headers=headers,
                params=params
            )

            data = response.json()
            items = data.get('items', [])

            if not items:
                break

            for item in items:
                channel_id = item.get('id')
                channel_name = item.get('title')

                if channel_id and channel_name:
                    all_channels.append(Channel(
                        channel_id=str(channel_id),
                        name=channel_name,
                        raw_data=item
                    ))

            # If we got fewer items than page_size, we've reached the last page
            if len(items) < page_size:
                break

            page += 1

        self.logger.info(f"📺 从 MeWatch 发现 {len(all_channels)} 个频道")
        return all_channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch program data for all MeWatch channels with concurrency limit"""
        import asyncio

        concurrency = 5
        self.logger.info(f"📡 正在抓取 {len(channels)} 个 MeWatch 频道的节目数据 (并发数: {concurrency})")

        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(channel: Channel, session: aiohttp.ClientSession):
            async with semaphore:
                return await self._fetch_channel_programs(channel, session)

        # Use a single shared session for all requests
        timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)
        headers = self.get_default_headers()

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            # Create tasks for all channels
            tasks = [fetch_with_semaphore(channel, session) for channel in channels]

            # Execute all tasks concurrently (limited by semaphore)
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results and count errors
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

    async def _fetch_channel_programs(self, channel: Channel, session: aiohttp.ClientSession) -> List[Program]:
        """Fetch program data for a specific MeWatch channel (7 days concurrently)"""
        import asyncio

        self.logger.info(f"🔍【MeWatch】 正在获取频道节目: {channel.name} (ID: {channel.channel_id})")

        # Calculate date range (today to 6 days later)
        tz = ZoneInfo('Asia/Shanghai')
        today = datetime.now(tz).date()

        # Create tasks for all 7 days concurrently
        date_tasks = []
        for day_offset in range(7):
            target_date = today + timedelta(days=day_offset-1)
            date_str = target_date.strftime('%Y-%m-%d')
            date_tasks.append(self._fetch_day_programs(channel, date_str, session))

        # Execute all date requests concurrently
        results = await asyncio.gather(*date_tasks, return_exceptions=True)

        # Collect all programs and handle exceptions
        all_programs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                target_date = today + timedelta(days=i)
                date_str = target_date.strftime('%Y-%m-%d')
                self.logger.warning(f"⚠️ 获取 {channel.name} 在 {date_str} 的节目数据失败: {result}")
            else:
                all_programs.extend(result)

        self.logger.debug(f"📺 在 {channel.name} 中发现 {len(all_programs)} 个节目")
        return all_programs

    @retry(
        stop=stop_after_attempt(Config.HTTP_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.HTTP_RETRY_BACKOFF),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def _fetch_day_programs(self, channel: Channel, date_str: str, session: aiohttp.ClientSession) -> List[Program]:
        """Fetch program data for a specific channel on a specific day (async)"""
        schedules_url = f"{self.base_url}/schedules"

        params = {
            "channels": channel.channel_id,
            "date": date_str,
            "duration": "24",
            "ff": "idp,ldp,rpt,cd",
            "hour": "16",
            "intersect": "true",
            "lang": "zh",  # Request Chinese language data
            "segments": "all"
        }

        async with session.get(schedules_url, params=params) as response:
            data = await response.json()

        programs = []

        # Response is a list, each element contains schedules for a channel
        if isinstance(data, list) and len(data) > 0:
            channel_data = data[0]
            schedules = channel_data.get('schedules', [])

            for schedule in schedules:
                try:
                    item = schedule.get('item', {})

                    # Prefer secondaryLanguageTitle (Chinese), fallback to title (English)
                    title = item.get('secondaryLanguageTitle') or item.get('title', '')

                    # Add episode number if available
                    episode_number = item.get('episodeNumber')
                    if episode_number and title:
                        title = f"{title} - EP {episode_number}"

                    description = item.get('description', '')

                    # Convert UTC timestamps to Shanghai timezone
                    start_time_str = schedule.get('startDate')
                    end_time_str = schedule.get('endDate')

                    if not start_time_str or not end_time_str:
                        continue

                    # Parse ISO 8601 UTC timestamps (e.g., "2025-12-19T17:00:00Z")
                    # and convert to Shanghai timezone (UTC+8)
                    tz_shanghai = ZoneInfo('Asia/Shanghai')

                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    start_time = start_time.astimezone(tz_shanghai)

                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                    end_time = end_time.astimezone(tz_shanghai)

                    programs.append(Program(
                        channel_id=channel.channel_id,
                        title=title,
                        start_time=start_time,
                        end_time=end_time,
                        description=description,
                        raw_data=item
                    ))

                except Exception as e:
                    self.logger.warning(f"⚠️ 解析节目数据失败: {e}")
                    continue

        return programs


# Create platform instance
mewatch_platform = MeWatchPlatform()


# Legacy functions for backward compatibility
async def get_mewatch_epg():
    """Legacy function - fetch MeWatch EPG data"""
    try:
        channels = await mewatch_platform.fetch_channels()
        programs = await mewatch_platform.fetch_programs(channels)

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
        logger.error(f"❌ 旧版 get_mewatch_epg 函数错误: {e}", exc_info=True)
        return [], []