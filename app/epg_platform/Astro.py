import math
import pytz
from datetime import datetime, time, timezone, timedelta
from typing import List
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from ..logger import get_logger
from ..utils import has_chinese
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class AstroPlatform(BaseEPGPlatform):
    """Astro Go EPG platform implementation"""

    def __init__(self):
        super().__init__("astro")
        self.base_url = "https://sg-sg-sg.astro.com.my:9443"
        self.oauth_url = f"{self.base_url}/oauth2/authorize"
        self.channels_url = f"{self.base_url}/ctap/r1.6.0/shared/channels"
        self.grid_url = f"{self.base_url}/ctap/r1.6.0/shared/grid"

        self.referer = "https://astrogo.astro.com.my/"
        self.client_token = "v:1!r:80800!ur:GUEST_REGION!community:Malaysia%20Live!t:k!dt:PC!f:Astro_unmanaged!pd:CHROME-FF!pt:Adults"
        self.access_token = None

    async def fetch_channels(self) -> List[Channel]:
        """Fetch channel list from Astro Go API"""
        self.logger.info("📡 正在获取 Astro Go 频道列表")

        # Get access token first
        if not await self._get_access_token():
            raise Exception("❌ 获取 Astro 访问令牌失败")

        headers = self.get_default_headers({
            "Referer": self.referer,
            "Authorization": f"Bearer {self.access_token}",
            "Accept-Language": "zh"
        })

        params = {
            "clientToken": self.client_token
        }

        response = self.http_client.get(
            self.channels_url,
            headers=headers,
            params=params
        )

        data = response.json()
        channels = []

        for channel_data in data.get("channels", []):
            # Find logo URL
            logo = ""
            for media in channel_data.get("media", []):
                if media.get("type") == "regular":
                    logo = media.get("url", "")
                    break

            if not logo and channel_data.get("media"):
                logo = channel_data["media"][0].get("url", "")

            # Clean channel name
            channel_name = channel_data.get("name", "").replace(" HD", "").strip()

            channels.append(Channel(
                channel_id=str(channel_data.get("id", "")),
                name=channel_name,
                logo=logo,
                raw_data=channel_data
            ))

        self.logger.info(f"📺 发现 {len(channels)} 个 Astro Go 频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch program data for all channels"""
        self.logger.info(f"📡 正在抓取 {len(channels)} 个频道的节目数据")

        if not channels:
            return []

        # Get channel count and first channel ID for API query
        channel_count = len(channels)
        first_id = channels[0].channel_id

        # Fetch EPG for 7 days and merge
        merged_channels = {}

        for day in range(7):
            try:
                self.logger.info(f"🔍 【Astro】正在获取第 {day} 天的 EPG 数据")
                date_str, duration = self._get_date_params(day)

                raw_epg = await self._query_epg(date_str, duration, channel_count, first_id)

                if raw_epg.get("channels"):
                    for channel_data in raw_epg["channels"]:
                        channel_id = str(channel_data.get("id", ""))
                        schedule = channel_data.get("schedule", [])

                        if channel_id not in merged_channels:
                            merged_channels[channel_id] = []

                        merged_channels[channel_id].extend(schedule)

            except Exception as e:
                self.logger.error(f"❌ 获取第 {day} 天的 EPG 数据失败: {e}")
                continue

        # Convert to Program objects
        programs = []
        channel_lookup = {ch.channel_id: ch.name for ch in channels}

        for channel_id, schedule in merged_channels.items():
            channel_name = channel_lookup.get(channel_id, f"Channel {channel_id}")

            for program_data in schedule:
                try:
                    start_time_str = program_data.get("startDateTime")
                    duration = program_data.get("duration")

                    if start_time_str and duration:
                        start_time, end_time = self._parse_program_time(start_time_str, duration)

                        title = program_data.get("title", "")
                        description = program_data.get("synopsis", "")
                        episode_number = program_data.get("episodeNumber")

                        # Add episode number based on language
                        if episode_number:
                            if has_chinese(title) or has_chinese(description):
                                title += f" 第{episode_number}集"
                            else:
                                title += f" Ep{episode_number}"

                        programs.append(Program(
                            channel_id=channel_id,
                            title=title,
                            start_time=start_time,
                            end_time=end_time,
                            description=description,
                            raw_data=program_data
                        ))

                except Exception as e:
                    self.logger.warning(f"⚠️ 解析节目数据失败: {e}")
                    continue

        self.logger.info(f"📊 总共抓取了 {len(programs)} 个节目")
        return programs

    async def _get_access_token(self) -> bool:
        """Get OAuth access token for Astro API"""
        try:
            params = {
                "client_id": "browser",
                "state": "guestUserLogin",
                "response_type": "token",
                "redirect_uri": "https://astrogo.astro.com.my",
                "scope": "urn:synamedia:vcs:ovp:guest-user",
                "prompt": "none",
            }

            headers = self.get_default_headers({
                "Referer": self.referer,
            })

            response = self.http_client.get(
                self.oauth_url,
                headers=headers,
                params=params,
                allow_redirects=False  # We need to handle redirect manually
            )

            location = response.headers.get("Location")
            if not location:
                self.logger.error("❌ OAuth 响应中未找到 Location 头")
                return False

            # Extract access token from fragment
            parsed = urlparse(location)
            fragment = parsed.fragment
            params = {}

            for item in fragment.split("&"):
                if "=" in item:
                    key, value = item.split("=", 1)
                    params[key] = value

            access_token = params.get("access_token")
            if access_token:
                self.access_token = access_token
                self.logger.debug(f"✨ 获取 Astro 访问令牌成功: {access_token[:20]}...")
                return True
            else:
                self.logger.error(f"❌ Location 片段中未找到访问令牌: {location}")
                return False

        except Exception as e:
            self.logger.error(f"❌ 获取 Astro 访问令牌失败: {e}")
            return False

    async def _query_epg(self, start_date: str, duration: int, channel_count: int, first_id: str) -> dict:
        """Query EPG data from Astro API"""
        headers = self.get_default_headers({
            "Referer": self.referer,
            "Authorization": f"Bearer {self.access_token}",
            "Accept-Language": "zh"
        })

        params = {
            "startDateTime": start_date,
            "channelId": first_id,
            "limit": channel_count,
            "genreId": "",
            "isPlayable": "true",
            "duration": duration,
            "clientToken": self.client_token
        }

        response = self.http_client.get(
            self.grid_url,
            headers=headers,
            params=params
        )

        if response.status_code == 200:
            return response.json()
        else:
            self.logger.warning(f"⚠️ EPG 查询失败，状态码: {response.status_code}")
            return {}

    def _get_date_params(self, date_delta: int) -> tuple:
        """Get date string and duration for EPG query"""
        now = datetime.now(ZoneInfo("Asia/Shanghai")) + timedelta(days=date_delta)

        if date_delta == 0:
            # For today, round down to nearest half-hour
            minute = now.minute
            rounded_minute = 0 if minute < 30 else 30
            target_time = now.replace(minute=rounded_minute, second=0, microsecond=0)

            # Until midnight tomorrow
            next_day = (target_time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            dur_seconds = (next_day - target_time).total_seconds()
            duration = math.ceil(dur_seconds / 3600)  # Round up to hours
        else:
            # For other days, start from midnight
            target_time = datetime.combine(now.date(), time(0, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
            duration = 24

        # Convert to UTC ISO string
        target_time_utc = target_time.astimezone(timezone.utc)
        iso_str = target_time_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        return iso_str, duration

    def _parse_program_time(self, start_time_str: str, duration: int) -> tuple:
        """Parse program start time and calculate end time"""
        # Parse UTC start time
        start_time_utc = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S.000Z")
        start_time_utc = pytz.utc.localize(start_time_utc)

        # Calculate end time
        end_time_utc = start_time_utc + timedelta(seconds=duration)

        # Convert to Shanghai timezone
        shanghai_tz = pytz.timezone('Asia/Shanghai')
        start_time_local = start_time_utc.astimezone(shanghai_tz)
        end_time_local = end_time_utc.astimezone(shanghai_tz)

        return start_time_local, end_time_local


# Create platform instance
astro_platform = AstroPlatform()


# Legacy functions for backward compatibility
async def get_astro_epg():
    """Legacy function - fetch Astro EPG data"""
    try:
        channels = await astro_platform.fetch_channels()
        programs = await astro_platform.fetch_programs(channels)

        # Convert to legacy format
        raw_channels = []
        raw_programs = []

        for channel in channels:
            raw_channels.append({
                "channelName": channel.name,
                "channelId": channel.channel_id,
                "logo": channel.extra_data.get("logo", "")
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
        logger.error(f"❌ 旧版 get_astro_epg 函数错误: {e}", exc_info=True)
        return [], []


def find_channel_name_by_id(channels, channel_id):
    """Legacy utility function"""
    for channel in channels:
        if str(channel.get('channelId')) == str(channel_id):
            return channel.get('channelName')
    return f"Channel {channel_id}"


def utc_to_local(start_time, duration):
    """Legacy utility function"""
    return astro_platform._parse_program_time(start_time, duration)


def extract_fragment_params(location_url):
    """Legacy utility function"""
    parsed = urlparse(location_url)
    fragment = parsed.fragment
    params = dict()
    for item in fragment.split("&"):
        if "=" in item:
            key, value = item.split("=", 1)
            params[key] = value
    return params


def get_access_token():
    """Legacy utility function - synchronous version (limited functionality)"""
    logger.warning("⚠️ 调用了旧版 get_access_token - 请使用异步版本以获得更好的可靠性")
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("⚠️ 在异步上下文中无法运行同步令牌获取")
            return None
        else:
            success = loop.run_until_complete(astro_platform._get_access_token())
            return astro_platform.access_token if success else None
    except Exception as e:
        logger.error(f"❌ 旧版 get_access_token 错误: {e}")
        return None