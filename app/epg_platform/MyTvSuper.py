import pytz
from datetime import datetime, timedelta
from typing import List, Tuple

from ..config import Config
from ..logger import get_logger
from ..http_client import get_http_client
from ..utils import remove_brackets
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class MyTvSuperPlatform(BaseEPGPlatform):
    """MyTV Super (TVB) EPG platform implementation"""

    def __init__(self):
        super().__init__("tvb")
        self.base_url = "https://content-api.mytvsuper.com"

    async def fetch_channels(self) -> List[Channel]:
        """Fetch channel list from MyTV Super API"""
        self.logger.info("📺 正在从 MyTV Super 获取频道列表")

        headers = self.get_default_headers({
            "Origin": "https://www.mytvsuper.com",
            "Referer": "https://www.mytvsuper.com/",
            "Sec-CH-UA": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        })

        params = {
            "platform": "web",
            "country_code": "HK",
            "profile_class": "general",
        }

        response = self.http_client.get(
            f"{self.base_url}/v1/channel/list",
            headers=headers,
            params=params
        )

        data = response.json()
        channels = []

        for channel_data in data.get('channels', []):
            channel_name = remove_brackets(channel_data.get('name_tc', ''))
            if channel_name:
                channels.append(Channel(
                    channel_id=channel_data.get('network_code', ''),
                    name=channel_name,
                    network_code=channel_data.get('network_code', ''),
                    raw_data=channel_data
                ))

        self.logger.info(f"🎆 从 MyTV Super 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch program data for all channels"""
        self.logger.info(f"📡 正在获取 {len(channels)} 个频道的节目数据")

        all_programs = []
        for channel in channels:
            try:
                programs = await self._fetch_channel_programs(
                    channel.extra_data.get('network_code'),
                    channel.name
                )
                all_programs.extend(programs)
            except Exception as e:
                self.logger.error(f"❌ 获取频道 {channel.name} 的节目数据失败: {e}")
                continue

        self.logger.info(f"📊 共获取 {len(all_programs)} 个节目")
        return all_programs

    async def _fetch_channel_programs(self, network_code: str, channel_name: str) -> List[Program]:
        """Fetch program data for a specific channel"""
        self.logger.info(f"🔍 【myTV SUPER】 正在获取频道节目: {channel_name}")

        # Get date range (yesterday + 7 days)
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        end_date = (datetime.now() + timedelta(days=7)).strftime('%Y%m%d')

        headers = self.get_default_headers({
            "Origin": "https://www.mytvsuper.com",
            "Referer": "https://www.mytvsuper.com/",
        })

        params = {
            "epg_platform": "web",
            "country_code": "HK",
            "network_code": network_code,
            "from": start_date,
            "to": end_date,
        }

        response = self.http_client.get(
            f"{self.base_url}/v1/epg",
            headers=headers,
            params=params
        )

        data = response.json()
        programs = []

        # Flatten the EPG data structure
        total_epg = []
        for day_data in data:
            for item in day_data.get('item', []):
                epgs = item.get('epg', [])
                for epg in epgs:
                    total_epg.append(epg)

        # Process each program
        for i, epg_program in enumerate(total_epg):
            try:
                # Parse start time
                start_time_str = epg_program.get('start_datetime')
                if not start_time_str:
                    continue

                start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")

                # Calculate end time based on next program or default duration
                if i < len(total_epg) - 1:
                    next_epg = total_epg[i + 1]
                    next_start_time_str = next_epg.get('start_datetime')
                    if next_start_time_str:
                        end_time = datetime.strptime(next_start_time_str, "%Y-%m-%d %H:%M:%S")
                    else:
                        end_time = start_time + timedelta(minutes=30)
                else:
                    end_time = start_time + timedelta(minutes=30)

                # Convert to Asia/Shanghai timezone
                eastern_eight = pytz.timezone('Asia/Shanghai')
                start_time_with_tz = eastern_eight.localize(start_time)
                end_time_with_tz = eastern_eight.localize(end_time)

                programs.append(Program(
                    channel_id=network_code,
                    title=epg_program.get('programme_title_tc', ''),
                    start_time=start_time_with_tz,
                    end_time=end_time_with_tz,
                    description=epg_program.get('episode_synopsis_tc', ''),
                    raw_data=epg_program
                ))

            except Exception as e:
                self.logger.warning(f"⚠️ 解析节目数据失败: {e}")
                continue

        self.logger.debug(f"🎯 为 {channel_name} 找到 {len(programs)} 个节目")
        return programs


# Create platform instance
mytvsuper_platform = MyTvSuperPlatform()


# Legacy function for backward compatibility
async def get_channels(force: bool = False):
    """Legacy function - fetch channels and programs from MyTV Super"""
    try:
        channels = await mytvsuper_platform.fetch_channels()
        programs = await mytvsuper_platform.fetch_programs(channels)

        # Convert to legacy format
        raw_channels = [{"channelName": ch.name} for ch in channels]
        raw_programs = []

        for program in programs:
            raw_programs.append({
                "channelName": next((ch.name for ch in channels if ch.channel_id == program.channel_id), ""),
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time
            })

        return raw_channels, raw_programs

    except Exception as e:
        logger.error(f"💥 旧版 get_channels 函数错误: {e}", exc_info=True)
        return [], []


def utc8_to_utc(local_time: datetime):
    """Convert UTC+8 time to UTC (legacy utility function)"""
    eastern_eight = pytz.timezone('Asia/Shanghai')
    local_time_with_tz = eastern_eight.localize(local_time)
    utc_time = local_time_with_tz.astimezone(pytz.utc)
    return utc_time