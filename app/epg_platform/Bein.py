"""beIN SPORTS EPG client.

The beIN API is shared by several regional sites, but its response schema is
not quite consistent between regions.  This module deliberately keeps the
HTTP and parsing code together and accepts all of the schemas currently
returned by the API.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from ..config import Config
from ..logger import get_logger
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class BeinPlatform(BaseEPGPlatform):
    """Fetch beIN channels and schedules for all supported regions."""

    CHANNEL_URL = "https://www.beinsports.com/api/opta/tv-channel"
    EVENT_URL = "https://www.beinsports.com/api/opta/tv-event"

    # Keep this list explicit: it is also the source of truth for the regions
    # documented in README.md.  Regions which return no events are ignored.
    REGIONS = (
        "default", "en-mena", "ar-mena", "fr-fr", "en-us", "es-us",
        "en-hk", "en-ph", "en-th", "en-id", "en-my", "en-sg",
        "en-au", "en-nz", "bein-xtra",
    )

    # Short labels keep XML channel names readable while still making channel
    # names from different regional feeds unambiguous.  en-us is intentionally
    # rendered as [US], matching the requested output format.
    REGION_LABELS = {
        "default": "DEFAULT",
        "en-mena": "MENA",
        "ar-mena": "AR-MENA",
        "fr-fr": "FR",
        "en-us": "US",
        "es-us": "ES-US",
        "en-hk": "HK",
        "en-ph": "PH",
        "en-th": "TH",
        "en-id": "ID",
        "en-my": "MY",
        "en-sg": "SG",
        "en-au": "AU",
        "en-nz": "NZ",
        "bein-xtra": "XTRA",
    }

    # The API example supplied with this project covers one local calendar day
    # (00:00-24:00 Asia/Shanghai).  A one-day window keeps the request below
    # the API's 3000-event limit for MENA feeds.
    EPG_DAYS = 1

    def __init__(self):
        super().__init__("bein")
        self._region_channels: Dict[str, List[Channel]] = {}

    def _request_json(self, url: str, params: Any) -> Optional[dict]:
        """Issue one API request and return JSON, allowing a region to fail."""
        headers = self.get_default_headers({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.beinsports.com/",
        })
        try:
            response = self.http_client.get(url, headers=headers, params=params)
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("响应不是 JSON 对象")
            return payload
        except Exception as error:
            self.logger.warning(f"⚠️ Bein 请求失败 ({url}, {params}): {error}")
            return None

    @classmethod
    def _region_label(cls, region: str) -> str:
        return cls.REGION_LABELS.get(region, region.upper())

    async def fetch_channels(self) -> List[Channel]:
        """Fetch channels for every configured region.

        A failed region is skipped so that a transient API error cannot hide
        channels from the other regions.
        """
        self._region_channels = {}
        all_channels: List[Channel] = []

        async def fetch_region(region: str):
            payload = await asyncio.to_thread(
                self._request_json, self.CHANNEL_URL, {"region": region}
            )
            rows = payload.get("rows") if payload else []
            if not isinstance(rows, list):
                rows = []

            region_channels: List[Channel] = []
            label = self._region_label(region)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                data = row.get("data") or {}
                api_id = str(row.get("id") or "").strip()
                name = str(
                    row.get("name")
                    or data.get("Name")
                    or data.get("name")
                    or data.get("displayName")
                    or ""
                ).strip()
                if not api_id or not name:
                    continue

                # Prefix the API id with the region.  The same UUID can appear
                # in multiple feeds, and using a namespaced id prevents their
                # programmes from being merged accidentally.
                internal_id = f"{region}:{api_id}"
                region_channels.append(Channel(
                    channel_id=internal_id,
                    name=f"[{label}] {name}",
                    region=region,
                    api_id=api_id,
                    external_id=str(row.get("externalId") or "").strip(),
                    raw_data=row,
                ))

            # A few feeds (notably Malaysia) expose two channels with the same
            # display name.  Keep the requested simple name for unique entries,
            # and add the provider id only where needed so XML ids stay unique.
            name_counts = Counter(channel.name for channel in region_channels)
            for channel in region_channels:
                if name_counts[channel.name] > 1:
                    suffix = str(
                        channel.extra_data.get("external_id")
                        or channel.extra_data.get("api_id")
                    ).strip()
                    if suffix:
                        channel.name = f"{channel.name} ({suffix})"

            return region, region_channels

        # Regional endpoints are independent.  A small concurrency limit keeps
        # startup quick without opening one connection per region at once.
        semaphore = asyncio.Semaphore(5)

        async def fetch_region_limited(region: str):
            async with semaphore:
                return await fetch_region(region)

        results = await asyncio.gather(
            *(fetch_region_limited(region) for region in self.REGIONS),
            return_exceptions=True,
        )
        for region, result in zip(self.REGIONS, results):
            if isinstance(result, Exception):
                self.logger.warning(f"⚠️ Bein {region} 频道请求失败: {result}")
                continue
            _, region_channels = result
            if region_channels:
                self._region_channels[region] = region_channels
                all_channels.extend(region_channels)
                self.logger.info(f"📺 Bein {region} 发现 {len(region_channels)} 个频道")
            else:
                self.logger.info(f"⏭️ Bein {region} 没有可用频道，跳过")

        self.logger.info(f"📺 Bein 总共发现 {len(all_channels)} 个频道")
        return all_channels

    def _time_range(self) -> Tuple[datetime, datetime]:
        """Return the API window in UTC for the current Shanghai day."""
        # The endpoint accepts ISO strings and uses startBefore/endAfter.  Use
        # local midnight boundaries so the result matches the other EPG files.
        from zoneinfo import ZoneInfo

        local_tz = ZoneInfo(Config.EPG_TIMEZONE)
        local_start = datetime.now(local_tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        local_end = local_start + timedelta(days=self.EPG_DAYS)
        return local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc)

    @staticmethod
    def _iso_millis(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch and normalize events for each region in one request."""
        if not channels:
            return []

        # fetch_channels normally populates this mapping.  Rebuild it when a
        # caller supplies channels directly (useful for tests and integrations).
        grouped: Dict[str, List[Channel]] = {}
        for channel in channels:
            region = str(channel.extra_data.get("region") or "default")
            grouped.setdefault(region, []).append(channel)

        start, end = self._time_range()
        all_programs: List[Program] = []

        async def fetch_region_programs(region: str, region_channels: List[Channel]):
            api_ids = [str(c.extra_data.get("api_id") or c.channel_id).split(":", 1)[-1]
                       for c in region_channels]
            params: List[Tuple[str, str]] = [
                ("searchKey", ""),
                ("startBefore", self._iso_millis(end - timedelta(milliseconds=1))),
                ("endAfter", self._iso_millis(start)),
                ("limit", "3000"),
            ]
            params.extend(("channelIds", api_id) for api_id in api_ids)

            payload = await asyncio.to_thread(
                self._request_json, self.EVENT_URL, params
            )
            rows = payload.get("rows") if payload else []
            if not isinstance(rows, list) or not rows:
                self.logger.info(f"⏭️ Bein {region} 没有节目，跳过")
                return []

            by_key: Dict[str, Channel] = {}
            for channel in region_channels:
                api_id = str(channel.extra_data.get("api_id") or "")
                external_id = str(channel.extra_data.get("external_id") or "")
                raw = channel.extra_data.get("raw_data") or {}
                data = raw.get("data") or {}
                for key in (api_id, external_id, channel.channel_id,
                            str(data.get("Code") or ""),
                            str(data.get("StaticChannelCode") or ""),
                            channel.name):
                    if key:
                        by_key[key] = channel

            region_programs: List[Program] = []
            for row in rows:
                program = self._parse_program(row, by_key)
                if program is not None:
                    region_programs.append(program)
            self.logger.info(f"📊 Bein {region} 解析 {len(region_programs)}/{len(rows)} 个节目")
            return region_programs

        semaphore = asyncio.Semaphore(5)

        async def fetch_region_limited(region: str, region_channels: List[Channel]):
            async with semaphore:
                return await fetch_region_programs(region, region_channels)

        results = await asyncio.gather(
            *(fetch_region_limited(region, region_channels)
              for region, region_channels in grouped.items()),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                self.logger.warning(f"⚠️ Bein 区域节目请求失败: {result}")
            else:
                all_programs.extend(result)

        all_programs.sort(key=lambda item: (item.start_time, item.channel_id, item.end_time))
        self.logger.info(f"📊 Bein 总共抓取 {len(all_programs)} 个节目")
        return all_programs

    @classmethod
    def _parse_program(cls, row: dict, channel_map: Dict[str, Channel]) -> Optional[Program]:
        if not isinstance(row, dict):
            return None
        data = row.get("data") or {}
        channel_obj = row.get("channel") or {}
        channel_data = channel_obj.get("data") or {}

        channel_keys = (
            row.get("channelId"), channel_obj.get("id"), channel_obj.get("externalId"),
            data.get("channelId"), data.get("ChannelCode"), data.get("StaticChannelCode"),
            channel_obj.get("name"), channel_data.get("Name"),
        )
        channel = next((channel_map.get(str(key).strip()) for key in channel_keys
                        if key is not None and str(key).strip() in channel_map), None)
        if channel is None:
            return None

        title = cls._first_text(
            row.get("title"), data.get("title"), data.get("episodeTitle"),
            data.get("programName"), data.get("name"),
            cls._localized(data.get("Title")),
        )
        if not title:
            return None
        description = cls._first_text(
            row.get("description"), row.get("synopsis"), data.get("description"),
            data.get("synopsis"), cls._localized(data.get("Synopsis")),
            cls._localized(data.get("Remarks")),
        )

        start = cls._parse_datetime(
            row.get("startDate") or row.get("start") or row.get("eventDate")
            or data.get("startDate") or data.get("airingDateTimeUTC")
            or data.get("utcEventDate") or data.get("broadcastDate")
            or data.get("airingDateTime")
        )
        if start is None:
            return None
        end = cls._parse_datetime(
            row.get("endDate") or row.get("end") or data.get("endDate")
            or data.get("airingEndDateTimeUTC") or data.get("endDateTime")
        )
        if end is None:
            duration = cls._parse_duration(row.get("duration"))
            if duration is None:
                duration = cls._parse_duration(data.get("duration"))
            if duration is None:
                duration = cls._parse_duration(data.get("Duration"))
            if duration:
                end = start + duration
        if end is None or end <= start:
            return None

        return Program(
            channel_id=channel.channel_id,
            title=title,
            start_time=start,
            end_time=end,
            description=description,
            raw_data=row,
        )

    @staticmethod
    def _localized(value: Any) -> str:
        if isinstance(value, dict):
            return str(value.get("English") or value.get("en") or next(iter(value.values()), "") or "").strip()
        return str(value or "").strip()

    @staticmethod
    def _first_text(*values: Any) -> str:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            # API timestamps are milliseconds when large, seconds otherwise.
            seconds = float(value) / (1000 if abs(float(value)) > 100000000000 else 1)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        try:
            text = str(value).strip()
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            return None

    @staticmethod
    def _parse_duration(value: Any) -> Optional[timedelta]:
        if value is None or value == "":
            return None
        if isinstance(value, str) and ":" in value:
            try:
                parts = [float(part) for part in value.split(":")]
                if len(parts) == 3:
                    return timedelta(hours=parts[0], minutes=parts[1], seconds=parts[2])
            except ValueError:
                return None
        try:
            number = float(value)
            # Top-level duration is milliseconds; regional data.duration is
            # generally seconds.  Values under a day are therefore seconds.
            return timedelta(milliseconds=number if number > 100000 else number * 1000)
        except (TypeError, ValueError, OverflowError):
            return None


bein_platform = BeinPlatform()


async def get_bein_epg():
    """Fetch beIN data in the dictionary format consumed by EpgGenerator."""
    try:
        channels = await bein_platform.fetch_channels()
        if not channels:
            return [], []
        programs = await bein_platform.fetch_programs(channels)
        # "查不到就算了": do not publish empty channels from regions whose
        # schedule endpoint returned no events (or from individual channels
        # omitted by the event feed).
        active_ids = {program.channel_id for program in programs}
        channels = [channel for channel in channels if channel.channel_id in active_ids]
        channel_names = {channel.channel_id: channel.name for channel in channels}
        raw_channels = [
            {"channelName": channel.name, "channelId": channel.channel_id}
            for channel in channels
        ]
        raw_programs = [
            {
                "channelName": channel_names[program.channel_id],
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time,
            }
            for program in programs
            if program.channel_id in channel_names
        ]
        return raw_channels, raw_programs
    except Exception as error:
        logger.error(f"❌ get_bein_epg 函数错误: {error}", exc_info=True)
        return [], []
