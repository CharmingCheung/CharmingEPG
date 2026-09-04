"""Claro Brasil EPG client for the Premiere sports channels."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from ..config import Config
from ..logger import get_logger
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)


class ClaroPlatform(BaseEPGPlatform):
    """Fetch the seven Premiere channels from Claro's programme guide."""

    EPG_URL = "https://programacao.claro.com.br/gatekeeper/exibicao/select"
    CITY_ID = "1"
    EPG_DAYS_BEFORE = 1
    EPG_DAYS_AHEAD = 7

    # The Solr query uses ``id_revel`` values prefixed with ``1_``, while the
    # returned programme records identify channels using the numeric id_canal.
    CHANNELS = (
        ("1365", "Premiere Clubes", "https://mondrian.claro.com.br/channels/default/premiere.svg"),
        ("1360", "Premiere 2", "https://mondrian.claro.com.br/channels/default/premiere.svg"),
        ("1361", "Premiere 3", "https://mondrian.claro.com.br/channels/default/premiere.svg"),
        ("1362", "Premiere 4", "https://mondrian.claro.com.br/channels/default/premiere.svg"),
        ("1363", "Premiere 5", "https://mondrian.claro.com.br/channels/default/premiere.svg"),
        ("1364", "Premiere 6", "https://mondrian.claro.com.br/channels/default/premiere.svg"),
        ("693", "Premiere 7", "https://mondrian.claro.com.br/brands/channels/premiere.svg"),
    )

    def __init__(self):
        super().__init__("claro")

    async def fetch_channels(self) -> List[Channel]:
        """Return the fixed Premiere channel selection."""
        return [
            Channel(channel_id=channel_id, name=name, logo=logo)
            for channel_id, name, logo in self.CHANNELS
        ]

    def _request_json(self, params: Dict[str, str]) -> dict:
        response = self.http_client.get(
            self.EPG_URL,
            headers=self.get_default_headers({
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Referer": "https://programacao.claro.com.br/",
            }),
            params=params,
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Claro 接口响应不是 JSON 对象")
        return payload

    @classmethod
    def _request_params(cls, now: Optional[datetime] = None) -> Dict[str, str]:
        """Build the Solr query from yesterday through seven days ahead."""
        local_timezone = ZoneInfo(Config.EPG_TIMEZONE)
        current = now or datetime.now(local_timezone)
        if current.tzinfo is None:
            current = current.replace(tzinfo=local_timezone)
        else:
            current = current.astimezone(local_timezone)
        start_date = current.date() - timedelta(days=cls.EPG_DAYS_BEFORE)
        end_date = current.date() + timedelta(days=cls.EPG_DAYS_AHEAD)
        reveal_ids = " ".join(f"1_{channel_id}" for channel_id, _, _ in cls.CHANNELS)
        return {
            "q": f"id_revel:({reveal_ids}) AND id_cidade:{cls.CITY_ID}",
            "wt": "json",
            "rows": "100000",
            "start": "0",
            "sort": "id_canal asc,dh_inicio asc",
            "fl": "dh_fim dh_inicio st_titulo titulo id_programa id_canal id_cidade",
            "fq": (
                f"dh_inicio:[{start_date.isoformat()}T00:00:00Z TO "
                f"{end_date.isoformat()}T23:59:59Z]"
            ),
        }

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch and parse schedules from yesterday through seven days ahead."""
        if not channels:
            return []

        payload = await asyncio.to_thread(self._request_json, self._request_params())
        channel_ids = {channel.channel_id for channel in channels}
        programs = self._parse_programs(payload, channel_ids)
        self.logger.info(f"📊 总共抓取了 {len(programs)} 个 Claro Premiere 节目")
        return programs

    @classmethod
    def _parse_programs(cls, payload: dict, channel_ids: set[str]) -> List[Program]:
        response = payload.get("response") or {}
        docs = response.get("docs") or []
        if not isinstance(docs, list):
            raise ValueError("Claro 接口响应缺少 response.docs")

        programs = []
        for item in docs:
            if not isinstance(item, dict):
                continue
            channel_id = str(item.get("id_canal") or "").strip()
            title = str(item.get("titulo") or "").strip()
            if channel_id not in channel_ids or not title:
                continue

            try:
                start_time = cls._parse_utc(item.get("dh_inicio"))
                end_time = cls._parse_utc(item.get("dh_fim"))
            except (TypeError, ValueError):
                continue
            if end_time <= start_time:
                continue

            programs.append(Program(
                channel_id=channel_id,
                title=title,
                start_time=start_time,
                end_time=end_time,
            ))

        return sorted(
            programs,
            key=lambda program: (program.start_time, program.channel_id, program.end_time),
        )

    @staticmethod
    def _parse_utc(value: str) -> datetime:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Claro 节目时间为空")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


claro_platform = ClaroPlatform()


async def get_claro_epg():
    """Return Claro Premiere data in the format used by the XML generator."""
    try:
        channels = await claro_platform.fetch_channels()
        programs = await claro_platform.fetch_programs(channels)
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
        logger.error(f"❌ get_claro_epg 函数错误: {error}", exc_info=True)
        return [], []
