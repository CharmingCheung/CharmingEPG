"""Sling CMS EPG client for the general and sports channel groups."""

import asyncio
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

from ..config import Config
from ..logger import get_logger
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class SlingPlatform(BaseEPGPlatform):
    """Fetch Sling's unauthenticated CMS guide without playback credentials."""

    CMS_BASE = "https://cbd46b77.cdn.cms.movetv.com"
    SUMMARY_URL = f"{CMS_BASE}/cms/publish3/domain/summary/ums/1.json"
    SCHEDULE_URL = f"{CMS_BASE}/cms/publish3/channel/schedule/24/{{date}}/1/{{guid}}.json"
    CHANNEL_MAP = Path(__file__).with_name("data") / "sling_channels.json"
    CONCURRENCY = 50
    TEST_NAME = re.compile(
        r"(?:pilot|signal-test|atomizertest|opstest|mctest|stb-test)", re.I
    )

    def __init__(self, sports: bool = False):
        super().__init__("sling_sports" if sports else "sling")
        self.sports = sports
        self.days = 7 if sports else 3

    @staticmethod
    def _normalized(value) -> str:
        return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())

    @classmethod
    def _candidate_score(cls, local: dict, item: dict) -> tuple:
        metadata = item.get("metadata") or {}
        wanted = cls._normalized(local.get("name"))
        call_sign = cls._normalized(metadata.get("call_sign"))
        title = cls._normalized(item.get("title"))
        raw = " ".join(
            str(value or "")
            for value in (metadata.get("call_sign"), item.get("title"))
        )
        return (
            wanted in {call_sign, title},
            not bool(cls.TEST_NAME.search(raw)),
            bool(item.get("offered")),
            bool((item.get("visibility") or {}).get("visible", True)),
            item.get("primary_channel_guid") == item.get("channel_guid"),
        )

    @classmethod
    def _load_channel_map(cls) -> List[dict]:
        rows = json.loads(cls.CHANNEL_MAP.read_text(encoding="utf-8"))
        return [
            {"id": row[0], "name": row[1], "guid": row[2]}
            for row in rows
            if isinstance(row, list) and len(row) == 3
        ]

    def _request_json(self, url: str) -> dict:
        response = self.http_client.get(url)
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Sling CMS 响应不是 JSON 对象")
        return payload

    @classmethod
    def _match_catalog(cls, local_channels: List[dict], payload: dict) -> List[dict]:
        cms_channels = [
            item for item in payload.get("channels", [])
            if (item.get("metadata") or {}).get("is_linear_channel")
        ]
        by_id: Dict[str, List[dict]] = defaultdict(list)
        by_callsign: Dict[str, List[dict]] = defaultdict(list)
        for item in cms_channels:
            if item.get("dyna_source_id") is not None:
                by_id[str(item["dyna_source_id"])].append(item)
            metadata = item.get("metadata") or {}
            for value in (metadata.get("call_sign"), item.get("title")):
                normalized = cls._normalized(value)
                if normalized:
                    by_callsign[normalized].append(item)

        catalog = []
        for local in local_channels:
            choices = by_id.get(str(local.get("id")), [])
            if not choices:
                choices = by_callsign.get(cls._normalized(local.get("name")), [])
            if not choices:
                continue
            item = max(choices, key=lambda candidate: cls._candidate_score(local, candidate))
            metadata = item.get("metadata") or {}
            genres = metadata.get("genre") or []
            if isinstance(genres, str):
                genres = [genres]
            catalog.append({
                "id": local["guid"],
                "name": str(metadata.get("channel_name") or local["name"]).strip(),
                "cms_guid": item.get("channel_guid"),
                "category": str(genres[0]).strip() if genres else "Sling",
            })
        return catalog

    async def fetch_channels(self) -> List[Channel]:
        self.logger.info("📡 正在获取 Sling 频道列表")
        payload = await asyncio.to_thread(self._request_json, self.SUMMARY_URL)
        catalog = self._match_catalog(self._load_channel_map(), payload)
        selected = [
            item for item in catalog
            if (item["category"].casefold() == "sports") == self.sports
        ]
        self.logger.info(
            f"📺 Sling 匹配 {len(catalog)} 个频道，{self.platform_name} 选中 {len(selected)} 个"
        )
        return [
            Channel(
                channel_id=item["id"],
                name=item["name"],
                cms_guid=item["cms_guid"],
            )
            for item in selected
        ]

    @staticmethod
    def _date_strings(days: int, now: Optional[datetime] = None) -> List[str]:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        start = current.astimezone(timezone.utc).date()
        return [(start + timedelta(days=offset)).strftime("%Y%m%d") for offset in range(days)]

    async def _request_json_async(self, client: aiohttp.ClientSession, url: str) -> dict:
        attempts = max(1, Config.HTTP_MAX_RETRIES)
        last_error = None
        for attempt in range(attempts):
            try:
                async with client.get(url) as response:
                    response.raise_for_status()
                    payload = await response.json(content_type=None)
                    if not isinstance(payload, dict):
                        raise ValueError("Sling CMS 响应不是 JSON 对象")
                    return payload
            except Exception as error:
                last_error = error
                if attempt + 1 < attempts:
                    await asyncio.sleep(Config.HTTP_RETRY_BACKOFF * (2 ** attempt))
        raise RuntimeError(f"Sling CMS 请求失败: {last_error}") from last_error

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        dates = self._date_strings(self.days)
        total = len(channels) * len(dates)
        self.logger.info(
            f"📡 正在抓取 {len(channels)} 个 {self.platform_name} 频道的节目数据 "
            f"({len(dates)} 天，共 {total} 个请求，并发数: {self.CONCURRENCY})"
        )
        timeout = aiohttp.ClientTimeout(total=Config.HTTP_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit=self.CONCURRENCY,
            limit_per_host=self.CONCURRENCY,
            ttl_dns_cache=300,
        )
        results = []
        failed = 0
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self.get_default_headers(),
        ) as client:
            async def fetch_one(channel: Channel, date: str):
                url = self.SCHEDULE_URL.format(
                    date=date,
                    guid=channel.extra_data["cms_guid"],
                )
                try:
                    payload = await self._request_json_async(client, url)
                    return channel, date, payload, None
                except Exception as error:
                    return channel, date, None, error

            tasks = [
                asyncio.create_task(fetch_one(channel, date))
                for channel in channels
                for date in dates
            ]
            progress_every = max(1, total // 20)
            for completed, task in enumerate(asyncio.as_completed(tasks), 1):
                channel, date, payload, error = await task
                if error is not None:
                    failed += 1
                    self.logger.warning(
                        f"⚠️ 获取 {channel.name} {date} EPG 数据失败: {error}"
                    )
                else:
                    results.append((channel, payload))
                if completed % progress_every == 0 or completed == total:
                    self.logger.info(
                        f"📈 {self.platform_name} 抓取进度 {completed}/{total} "
                        f"(成功: {completed - failed}, 失败: {failed})"
                    )
        programs_by_key = {}
        for channel, payload in results:
            rows = ((payload.get("schedule") or {}).get("scheduleList") or [])
            for row in rows:
                program = self._parse_program(channel, row)
                if program is None:
                    continue
                key = (
                    row.get("schedule_guid"),
                    program.channel_id,
                    program.start_time,
                    program.title,
                )
                programs_by_key[key] = program

        programs = sorted(
            programs_by_key.values(),
            key=lambda item: (item.start_time, item.channel_id, item.end_time),
        )
        self.logger.info(
            f"📊 总共抓取了 {len(programs)} 个节目 "
            f"(成功请求: {total - failed}, 失败请求: {failed})"
        )
        return programs

    @staticmethod
    def _parse_program(channel: Channel, row: dict) -> Optional[Program]:
        title = str(row.get("title") or row.get("grid_title") or "").strip()
        try:
            start_time = datetime.fromtimestamp(int(row["schedule_start"]), timezone.utc)
            stop_value = row.get("schedule_stop")
            if stop_value is None:
                stop_value = int(row["schedule_start"]) + int(row["duration"])
            end_time = datetime.fromtimestamp(int(stop_value), timezone.utc)
        except (KeyError, TypeError, ValueError, OSError):
            return None
        if not title or end_time <= start_time:
            return None

        metadata = row.get("metadata") or {}
        program_data = row.get("program") or {}
        subtitle = str(
            metadata.get("episode_title") or program_data.get("name") or ""
        ).strip()
        if subtitle and subtitle != title:
            title = f"{title} : {subtitle}"
        return Program(
            channel_id=channel.channel_id,
            title=title,
            start_time=start_time,
            end_time=end_time,
            description=str(
                metadata.get("short_description")
                or metadata.get("description")
                or program_data.get("short_description")
                or ""
            ),
        )


sling_platform = SlingPlatform(sports=False)
sling_sports_platform = SlingPlatform(sports=True)


async def get_sling_epg(sports: bool = False):
    """Return Sling data in the format used by the common XML generator."""
    try:
        platform = sling_sports_platform if sports else sling_platform
        channels = await platform.fetch_channels()
        programs = await platform.fetch_programs(channels)
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
        logger.error(f"❌ get_sling_epg 函数错误: {error}", exc_info=True)
        return [], []
