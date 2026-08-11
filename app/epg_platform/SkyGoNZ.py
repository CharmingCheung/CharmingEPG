import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

import aiohttp
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import Config
from ..logger import get_logger
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class SkyGoNZPlatform(BaseEPGPlatform):
    """SkyGo New Zealand EPG platform implementation."""

    GRAPHQL_URL = "https://api.skyone.co.nz/exp/graph"
    EPG_DAYS = 7
    GRAPHQL_QUERY = """
        query getSlots($from: DateTime! $to: DateTime!) {
          channels {
            __typename
            ... on LinearChannel {
              id
              title
              number
              slots(from: $from to: $to) {
                id
                start
                end
                programme {
                  __typename
                  ... on Title {
                    id
                    title
                  }
                  ... on Movie {
                    primaryGenres { title }
                    year
                  }
                  ... on Episode {
                    id
                    title
                    number
                    show {
                      id
                      title
                      type
                      primaryGenres { title }
                    }
                    season { id number }
                  }
                  ... on PayPerViewEventProgram {
                    id
                    title
                  }
                }
              }
            }
          }
        }
    """

    def __init__(self):
        super().__init__("skygonz")

    @retry(
        stop=stop_after_attempt(Config.HTTP_MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.HTTP_RETRY_BACKOFF),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _fetch_guide(self) -> List[dict]:
        """Fetch all linear channels and their seven-day schedules."""
        from_time, to_time = self._get_time_range()
        params = {
            "query": self.GRAPHQL_QUERY,
            "variables": (
                f'{{"from":"{self._format_utc(from_time)}",'
                f'"to":"{self._format_utc(to_time)}"}}'
            ),
            "operationName": "getSlots",
        }
        headers = self.get_default_headers({
            "Accept": "application/json",
            "Origin": "https://www.skygo.co.nz",
            "Referer": "https://www.skygo.co.nz/",
        })
        timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)

        self.logger.info(
            f"📡 正在从 SkyGo NZ 获取 {self.EPG_DAYS} 天节目数据: "
            f"{params['variables']}"
        )

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(self.GRAPHQL_URL, params=params) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)

        errors = payload.get("errors") if isinstance(payload, dict) else None
        if errors:
            raise RuntimeError(f"SkyGo NZ GraphQL 接口返回错误: {errors}")

        channels = (payload.get("data") or {}).get("channels") if isinstance(payload, dict) else None
        if channels is None:
            raise RuntimeError(f"SkyGo NZ 接口响应异常: {str(payload)[:200]}")

        return channels

    async def fetch_channels(self) -> List[Channel]:
        """Fetch linear channels with their embedded schedule data."""
        raw_channels = await self._fetch_guide()
        channels = []

        for item in raw_channels:
            if item.get("__typename") != "LinearChannel":
                continue

            channel_id = str(item.get("id") or "").strip()
            channel_name = str(item.get("title") or "").strip()
            if not channel_id or not channel_name:
                continue

            channels.append(Channel(
                channel_id=channel_id,
                name=channel_name,
                number=item.get("number"),
                slots=item.get("slots") or [],
            ))

        self.logger.info(f"📺 从 SkyGo NZ 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Parse programs embedded in the channel response."""
        programs = []

        for channel in channels:
            for slot in channel.extra_data.get("slots", []):
                try:
                    programme = slot.get("programme") or {}
                    title = self._format_program_title(programme)
                    start_raw = slot.get("start")
                    end_raw = slot.get("end")
                    if not title or not start_raw or not end_raw:
                        continue

                    programs.append(Program(
                        channel_id=channel.channel_id,
                        title=title,
                        start_time=self._parse_datetime(start_raw),
                        end_time=self._parse_datetime(end_raw),
                        raw_data=slot,
                    ))
                except (TypeError, ValueError) as error:
                    self.logger.warning(f"⚠️ 解析 SkyGo NZ 节目数据失败: {error}")

        self.logger.info(f"📊 总共抓取了 {len(programs)} 个 SkyGo NZ 节目")
        return programs

    @classmethod
    def _get_time_range(cls):
        nz_timezone = ZoneInfo("Pacific/Auckland")
        start = datetime.now(nz_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, start + timedelta(days=cls.EPG_DAYS)

    @staticmethod
    def _format_utc(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    @classmethod
    def _format_program_title(cls, programme: dict) -> str:
        episode_title = str(programme.get("title") or "").strip()
        if programme.get("__typename") != "Episode":
            return episode_title

        show_title = str((programme.get("show") or {}).get("title") or "").strip()
        season_number = (programme.get("season") or {}).get("number")
        episode_number = programme.get("number")
        episode_code = cls._format_episode_code(season_number, episode_number)

        episode_detail = f"({episode_code}){episode_title}" if episode_code else episode_title
        if show_title and episode_detail and show_title != episode_title:
            return f"{show_title} - {episode_detail}"
        return show_title or episode_detail

    @staticmethod
    def _format_episode_code(season_number: Optional[int], episode_number: Optional[int]) -> str:
        parts = []
        if season_number is not None:
            parts.append(f"S{int(season_number):02d}")
        if episode_number is not None:
            parts.append(f"E{int(episode_number):02d}")
        return "".join(parts)


skygonz_platform = SkyGoNZPlatform()


async def get_skygonz_epg():
    """Fetch SkyGo NZ EPG data in the format used by the XML generator."""
    try:
        channels = await skygonz_platform.fetch_channels()
        if not channels:
            return [], []

        programs = await skygonz_platform.fetch_programs(channels)
        channel_names = {channel.channel_id: channel.name for channel in channels}

        raw_channels = [
            {"channelName": channel.name, "channelId": channel.channel_id}
            for channel in channels
        ]
        raw_programs = [
            {
                "channelName": channel_names.get(program.channel_id, program.channel_id),
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time,
            }
            for program in programs
        ]
        return raw_channels, raw_programs
    except Exception as error:
        logger.error(f"❌ get_skygonz_epg 函数错误: {error}", exc_info=True)
        return [], []
