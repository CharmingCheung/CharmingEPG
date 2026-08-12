import asyncio
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from ..config import Config
from ..logger import get_logger
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class TelusPlatform(BaseEPGPlatform):
    """TELUS TV+ EPG platform implementation."""

    BASE_URL = (
        "https://telus.prod.g.telustvplus.com/"
        "TELUS/T7.3/A/ENG/CHROME_FIREFOX_HTML5/OPTIK/TRAY"
    )
    WEBSITE_URL = "https://www.telustvplus.com/"
    EPG_DAYS = 7
    IMPERSONATE = "chrome120"

    def __init__(self):
        super().__init__("telus")
        self._session = None

    def _get_session(self):
        """Create and warm up a browser-impersonating session for Cloudflare."""
        if self._session is not None:
            return self._session

        try:
            from curl_cffi import requests as cffi_requests
        except ImportError as error:
            raise RuntimeError(
                "缺少依赖 curl_cffi，无法抓取 Telus，请执行 pip install curl_cffi"
            ) from error

        session = cffi_requests.Session()
        try:
            session.get(
                self.WEBSITE_URL,
                timeout=Config.HTTP_TIMEOUT,
                impersonate=self.IMPERSONATE,
            )
        except Exception as error:
            self.logger.debug(f"Telus 首页预热失败，将继续请求 API: {error}")

        self._session = session
        return session

    def _request_json(self, endpoint: str, params: Dict[str, str]) -> dict:
        """Fetch one API response using a Chrome TLS fingerprint."""
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-CA,en;q=0.9",
            "Cache-Control": "no-cache",
            "Origin": self.WEBSITE_URL.rstrip("/"),
            "Pragma": "no-cache",
            "Referer": self.WEBSITE_URL,
            "Restful": "yes",
        }
        url = f"{self.BASE_URL}/{endpoint}"
        attempts = max(1, Config.HTTP_MAX_RETRIES)
        last_error = None

        for attempt in range(attempts):
            try:
                response = self._get_session().get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=Config.HTTP_TIMEOUT,
                    impersonate=self.IMPERSONATE,
                )

                if response.status_code == 403:
                    self._session = None
                    raise RuntimeError("HTTP 403 (Cloudflare 拦截)")
                response.raise_for_status()

                payload = response.json()
                if not isinstance(payload, dict) or payload.get("resultCode") != "OK":
                    message = payload.get("message") if isinstance(payload, dict) else ""
                    raise RuntimeError(f"Telus API 返回异常: {message or str(payload)[:200]}")
                if not isinstance(payload.get("resultObj"), dict):
                    raise RuntimeError("Telus API 响应缺少 resultObj")
                return payload
            except Exception as error:
                last_error = error
                if attempt + 1 >= attempts:
                    break
                wait_seconds = Config.HTTP_RETRY_BACKOFF * (2 ** attempt)
                self.logger.warning(
                    f"⚠️ Telus 请求失败 ({error})，{wait_seconds:.1f} 秒后重试 "
                    f"({attempt + 1}/{attempts})"
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"Telus 请求失败: {last_error}") from last_error

    async def fetch_channels(self) -> List[Channel]:
        """Fetch all channels available in the configured Telus region."""
        self.logger.info(f"📡 正在获取 Telus 区域 {Config.TELUS_REGION_ID} 的频道列表")
        payload = await asyncio.to_thread(
            self._request_json,
            "LIVECHANNELS",
            {
                "orderBy": "orderId",
                "sortOrder": "asc",
                "filter_regionId": str(Config.TELUS_REGION_ID),
            },
        )
        channels = self._parse_channels(payload)
        self.logger.info(f"📺 从 Telus 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch seven days of programs in daily requests."""
        channel_ids = {channel.channel_id for channel in channels}
        programs_by_key = {}
        time_ranges = self._get_time_ranges()

        for index, (start, end) in enumerate(time_ranges, start=1):
            self.logger.info(
                f"📡 正在获取 Telus EPG ({index}/{len(time_ranges)}): "
                f"{start.date().isoformat()}"
            )
            payload = await asyncio.to_thread(
                self._request_json,
                "EPG",
                {
                    "filter_startTime": str(self._to_milliseconds(start)),
                    "filter_endTime": str(self._to_milliseconds(end)),
                    "filter_regionId": str(Config.TELUS_REGION_ID),
                },
            )

            for program in self._parse_programs(payload, channel_ids):
                key = (
                    program.channel_id,
                    program.start_time,
                    program.end_time,
                    program.title,
                )
                programs_by_key[key] = program

        programs = sorted(
            programs_by_key.values(),
            key=lambda item: (item.start_time, item.channel_id, item.end_time),
        )
        self.logger.info(f"📊 总共抓取了 {len(programs)} 个 Telus 节目")
        return programs

    @classmethod
    def _parse_channels(cls, payload: dict) -> List[Channel]:
        containers = (payload.get("resultObj") or {}).get("containers") or []
        if not isinstance(containers, list):
            return []

        parsed = []
        for item in containers:
            metadata = item.get("metadata") or {}
            channel_id = str(metadata.get("channelId") or item.get("id") or "").strip()
            name = str(metadata.get("channelName") or "").strip()
            if not channel_id or not name:
                continue

            assets = item.get("assets") or []
            logo = ""
            for asset in assets:
                logo = str(
                    asset.get("logoBig")
                    or asset.get("logoMedium")
                    or asset.get("logoSmall")
                    or ""
                ).strip()
                if logo:
                    break

            parsed.append((channel_id, name, metadata, logo))

        name_counts = Counter(name for _, name, _, _ in parsed)
        channels = []
        for channel_id, name, metadata, logo in parsed:
            display_name = name if name_counts[name] == 1 else f"{name} ({channel_id})"
            channels.append(Channel(
                channel_id=channel_id,
                name=display_name,
                original_name=name,
                channel_number=metadata.get("defaultChannelNumber"),
                call_letter=metadata.get("callLetter"),
                logo=logo,
                raw_data=metadata,
            ))
        return channels

    @classmethod
    def _parse_programs(cls, payload: dict, channel_ids: set) -> List[Program]:
        channel_rows = (payload.get("resultObj") or {}).get("containers") or []
        if not isinstance(channel_rows, list):
            return []

        programs = []
        for row in channel_rows:
            row_metadata = row.get("metadata") or {}
            row_channel_id = str(
                row_metadata.get("channelId") or row.get("id") or ""
            ).strip()

            for item in row.get("containers") or []:
                metadata = item.get("metadata") or {}
                item_channel = item.get("channel") or {}
                channel_id = str(item_channel.get("channelId") or row_channel_id).strip()
                if channel_id not in channel_ids:
                    continue

                title = str(metadata.get("title") or "").strip()
                description = str(metadata.get("longDescription") or "").strip()
                start_ms = metadata.get("airingStartTime")
                end_ms = metadata.get("airingEndTime")
                if not title or start_ms is None or end_ms is None:
                    continue

                try:
                    start_time = cls._from_milliseconds(start_ms)
                    end_time = cls._from_milliseconds(end_ms)
                except (TypeError, ValueError, OSError):
                    continue
                if end_time <= start_time:
                    continue

                programs.append(Program(
                    channel_id=channel_id,
                    title=title,
                    start_time=start_time,
                    end_time=end_time,
                    description=description,
                ))
        return programs

    @classmethod
    def _get_time_ranges(
        cls, now: Optional[datetime] = None
    ) -> List[Tuple[datetime, datetime]]:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        start = current.astimezone(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return [
            (start + timedelta(days=offset), start + timedelta(days=offset + 1))
            for offset in range(cls.EPG_DAYS)
        ]

    @staticmethod
    def _to_milliseconds(value: datetime) -> int:
        return int(value.timestamp() * 1000)

    @staticmethod
    def _from_milliseconds(value) -> datetime:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


telus_platform = TelusPlatform()


async def get_telus_epg():
    """Fetch Telus EPG data in the format used by the XML generator."""
    try:
        channels = await telus_platform.fetch_channels()
        if not channels:
            return [], []

        programs = await telus_platform.fetch_programs(channels)
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
        logger.error(f"❌ get_telus_epg 函数错误: {error}", exc_info=True)
        return [], []
