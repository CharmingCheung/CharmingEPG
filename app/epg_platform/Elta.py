"""ELTA OTT schedules from the public JSON feeds used by program_detail."""

import asyncio
from datetime import datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

from .base import BaseEPGPlatform, Channel, Program


class EltaPlatform(BaseEPGPlatform):
    BASE_URL = "https://piceltaott-elta.cdn.hinet.net/production/json"
    CHANNEL_URL = f"{BASE_URL}/channel/channel_list.json"
    PROGRAM_URL = f"{BASE_URL}/program_list/program_list_{{date}}.json"
    TIMEZONE = ZoneInfo("Asia/Taipei")
    # Match the website's date selector: two previous days through four ahead.
    DAY_OFFSETS = range(-2, 5)
    # Only ELTA's own channels receive the brand prefix. The OTT package also
    # carries third-party channels whose original names must be preserved.
    CHANNEL_NAMES = {
        "101": "ELTA體育1台",
        "105": "ELTA體育2台",
        "110": "ELTA體育3台",
        "115": "ELTA體育4台",
        **{str(539 + number): f"ELTA體育MAX{number}台" for number in range(1, 11)},
        "102": "ELTA影劇台",
        "103": "ELTA綜合台",
        "104": "ELTA寵物運動台",
    }

    def __init__(self):
        super().__init__("elta")

    def _request_json(self, url: str):
        return self.http_client.get(
            url, headers=self.get_default_headers({
                "Referer": "https://eltaott.tv/channel/program_detail",
            }),
        ).json()

    @classmethod
    def _parse_channels(cls, payload, now: datetime) -> List[Channel]:
        if not isinstance(payload, list):
            raise ValueError("Elta 频道列表不是 JSON 数组")
        channels = []
        seen = set()
        now = now.astimezone(cls.TIMEZONE)
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("cl_status")) != "1" or str(item.get("cl_display")) != "1":
                continue
            show_type = str(item.get("cp_show_type"))
            if show_type == "2":
                try:
                    start = datetime.fromisoformat(item["cp_schedule_from_datetime"]).replace(tzinfo=cls.TIMEZONE)
                    end = datetime.fromisoformat(item["cp_schedule_to_datetime"]).replace(tzinfo=cls.TIMEZONE)
                except (KeyError, TypeError, ValueError):
                    continue
                if not start <= now < end:
                    continue
            elif show_type != "1":
                continue
            channel_id = str(item.get("cl_num") or "").strip()
            name = str(item.get("cl_title") or "").strip()
            if channel_id and name and channel_id not in seen:
                name = cls.CHANNEL_NAMES.get(channel_id, name)
                channels.append(Channel(channel_id=channel_id, name=name))
                seen.add(channel_id)
        return channels

    async def fetch_channels(self) -> List[Channel]:
        payload = await asyncio.to_thread(self._request_json, self.CHANNEL_URL)
        channels = self._parse_channels(payload, datetime.now(self.TIMEZONE))
        self.logger.info(f"📺 从 Elta 发现 {len(channels)} 个频道")
        return channels

    @classmethod
    def _parse_programs(cls, payload, channel_ids: set[str]) -> List[Program]:
        if not isinstance(payload, dict):
            raise ValueError("Elta 节目表不是 JSON 对象")
        programs = []
        for channel_id, entries in payload.items():
            channel_id = str(channel_id)
            if channel_id not in channel_ids or not isinstance(entries, list):
                continue
            for item in entries:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("program_desc") or "").strip()
                try:
                    start_ts = int(item["start_time"])
                    end_ts = int(item["end_time"])
                    if not title or start_ts <= 0 or end_ts <= start_ts:
                        continue
                    start = datetime.fromtimestamp(start_ts, cls.TIMEZONE)
                    end = datetime.fromtimestamp(end_ts, cls.TIMEZONE)
                except (KeyError, TypeError, ValueError, OverflowError, OSError):
                    continue
                # program_desc is the programme title, rather than a synopsis.
                programs.append(Program(channel_id, title, start, end))
        return programs

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        if not channels:
            return []
        today = datetime.now(self.TIMEZONE).date()
        channel_ids = {channel.channel_id for channel in channels}
        programs = []
        seen = set()
        for offset in self.DAY_OFFSETS:
            date = (today + timedelta(days=offset)).isoformat()
            try:
                payload = await asyncio.to_thread(self._request_json, self.PROGRAM_URL.format(date=date))
                daily_programs = self._parse_programs(payload, channel_ids)
                if offset == 0 and not daily_programs:
                    raise ValueError("Elta 今日节目表没有有效节目")
            except Exception as error:
                # Do not cache an EPG without today's data; other dates may not
                # be published yet and must not prevent serving today's guide.
                if offset == 0:
                    raise
                self.logger.warning(f"⚠️ Elta {date} 节目表获取失败: {error}")
                continue
            for program in daily_programs:
                key = (program.channel_id, program.start_time, program.end_time, program.title)
                if key not in seen:
                    seen.add(key)
                    programs.append(program)
        programs.sort(key=lambda program: (program.channel_id, program.start_time, program.end_time))
        self.logger.info(f"📊 总共抓取了 {len(programs)} 个 Elta 节目")
        return programs


async def get_elta_epg():
    """Return channel names and programmes in the existing XML generator format."""
    platform = EltaPlatform()
    try:
        channels = await platform.fetch_channels()
        programs = await platform.fetch_programs(channels)
        names = {channel.channel_id: channel.name for channel in channels}
        return (
            [{"channelName": channel.name, "channelId": channel.channel_id} for channel in channels],
            [{
                "channelName": names[program.channel_id],
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time,
            } for program in programs],
        )
    except Exception as error:
        platform.logger.error(f"❌ 获取 Elta EPG 失败: {error}", exc_info=True)
        return [], []
