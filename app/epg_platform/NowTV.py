import asyncio
import time
import pytz
from datetime import datetime
from typing import List, Dict

from ..logger import get_logger
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)

_CATALOG_API = "https://catalogapi.nowtv.now.com/CatalogEngine"
_PLATFORM = "NPX"
_LANG = "zh"
_APP_ID = "15"
_DETAIL_CHUNK_SIZE = 500


def _ref_no() -> str:
    return f"Ad449480d{int(time.time() * 1000)}"


class NowTVPlatform(BaseEPGPlatform):
    """NowTV EPG platform — uses the app catalog API"""

    def __init__(self):
        super().__init__("nowtv")

    def _post(self, endpoint: str, payload: dict) -> dict:
        headers = self.get_default_headers({
            "Host": "catalogapi.nowtv.now.com",
            "Content-Type": "application/json; charset=utf-8",
        })
        response = self.http_client.post(
            f"{_CATALOG_API}/{endpoint}",
            json=payload,
            headers=headers,
        )
        return response.json()

    async def fetch_channels(self) -> List[Channel]:
        self.logger.info("📡 正在从 NowTV 获取频道列表")
        data = self._post("getLiveChannelList", {
            "appId": _APP_ID,
            "lang": _LANG,
            "callerReferenceNo": _ref_no(),
            "platform": _PLATFORM,
        })

        channels = []
        for item in data.get("channelList", []):
            logo = item.get("channelLogoLink") or None
            channels.append(Channel(
                channel_id=item["channelId"],
                name=item["name"],
                logo=logo,
            ))

        self.logger.info(f"📺 从 NowTV 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        self.logger.info(f"📡 正在抓取 {len(channels)} 个频道的节目数据")
        channel_ids = [ch.channel_id for ch in channels]

        # All channels × 7 days in a single request
        data = self._post("getEPGDetail", {
            "channelIdList": channel_ids,
            "startDay": 0,
            "endDay": 6,
            "lang": _LANG,
            "callerReferenceNo": _ref_no(),
            "platform": _PLATFORM,
        })

        # Collect raw entries and gather all programIds for batch detail fetch
        raw: List[tuple] = []  # (channel_id_str, prog_dict)
        prog_ids: List[str] = []
        for channel_epg in data.get("epgDetail", []):
            cid = str(channel_epg["channelId"])
            for prog in channel_epg.get("programs", []):
                raw.append((cid, prog))
                prog_ids.append(str(prog["vimProgramId"]))

        self.logger.info(f"📊 共获取到 {len(raw)} 个节目，正在批量获取节目详情")
        details = await self._fetch_program_details(prog_ids)

        programs = []
        for cid, prog in raw:
            try:
                detail = details.get(str(prog["vimProgramId"]), {})
                desc = detail.get("chiSynopsis") or detail.get("synopsis") or ""
                start = _ts_to_dt(prog["start"] / 1000)
                end = _ts_to_dt(prog["end"] / 1000)
                programs.append(Program(
                    channel_id=cid,
                    title=prog.get("name", ""),
                    start_time=start,
                    end_time=end,
                    description=desc,
                ))
            except Exception as e:
                self.logger.warning(f"⚠️ 解析节目数据失败: {e}")

        self.logger.info(f"✅ 共生成 {len(programs)} 个节目")
        return programs

    async def _fetch_program_details(self, prog_ids: List[str]) -> Dict[str, dict]:
        chunks = [prog_ids[i:i + _DETAIL_CHUNK_SIZE] for i in range(0, len(prog_ids), _DETAIL_CHUNK_SIZE)]
        semaphore = asyncio.Semaphore(40)

        async def fetch_chunk(chunk: List[str]) -> List[dict]:
            async with semaphore:
                try:
                    data = await asyncio.to_thread(self._post, "getEPGProgramDetailList", {
                        "lang": _LANG,
                        "programIdList": chunk,
                        "callerReferenceNo": _ref_no(),
                        "platform": _PLATFORM,
                    })
                    return data.get("epgProgramList", [])
                except Exception as e:
                    self.logger.warning(f"⚠️ 批量获取节目详情失败: {e}")
                    return []

        results = await asyncio.gather(*[fetch_chunk(c) for c in chunks])
        details: Dict[str, dict] = {}
        for items in results:
            for item in items:
                details[str(item["programId"])] = item
        return details


def _ts_to_dt(timestamp: float) -> datetime:
    utc_dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
    return utc_dt.astimezone(pytz.timezone("Asia/Shanghai"))


# Platform instance
nowtv_platform = NowTVPlatform()


async def request_nowtv_today_epg() -> bytes:
    """Fetch NowTV EPG as XML bytes"""
    from ..epg.EpgGenerator import generateEpg
    try:
        channels = await nowtv_platform.fetch_channels()
        programs = await nowtv_platform.fetch_programs(channels)

        id_to_name = {ch.channel_id: ch.name for ch in channels}
        raw_channels = [{"channelName": ch.name, "channelId": ch.channel_id} for ch in channels]
        raw_programs = [{
            "channelName": id_to_name.get(p.channel_id, p.channel_id),
            "programName": p.title,
            "description": p.description,
            "start": p.start_time,
            "end": p.end_time,
        } for p in programs]

        return await generateEpg(raw_channels, raw_programs)
    except Exception as e:
        logger.error(f"❌ request_nowtv_today_epg 错误: {e}", exc_info=True)
        return b""
