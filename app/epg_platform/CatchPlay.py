import asyncio
from datetime import datetime, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from ..logger import get_logger
from ..config import Config
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)

# CatchPlay 的节目表接口需要台湾 IP，非台湾 IP 会返回：
#   {"code": "core-0100", "message": "The location XX is not allowed."}
# 因此需要通过 CATCHPLAY_PROXY 配置台湾代理（用法与 SINGTEL_PROXY 一致）。
#
# 整个节目表（约 3 天、上百个频道）通过一次 GraphQL 请求即可全部取回，
# 每个频道的 epg 内嵌在 channels[].epg 中，start/end 为 ISO UTC 时间。


class CatchPlayPlatform(BaseEPGPlatform):
    """CatchPlay (台湾) EPG platform implementation.

    单个 GraphQL 接口一次性返回全部频道与节目表，接口有台湾地区 IP 限制。
    """

    GRAPHQL_URL = "https://sunapi.catchplay.com/program/v3/graphql"

    # 精简后仅需 3 个 header：authorization / content-type / asiaplay-device-type
    # authorization 直接使用固定 Basic（跳过换 Bearer token 那一步）
    # asiaplay-device-type 值非空即可
    AUTHORIZATION = "Basic NTQ3MzM0NDgtYTU3Yi00MjU2LWE4MTEtMzdlYzNkNjJmM2E0Ok90QzR3elJRR2hLQ01sSDc2VEoy"

    GRAPHQL_QUERY = (
        "query getChannelPackage ($channelGenreId: ID) {\n"
        "    getChannelPackage (channelGenreId: $channelGenreId) {\n"
        "        channels {\n"
        "            id\n"
        "            title {\n"
        "                local\n"
        "            }\n"
        "            epg {\n"
        "                epgTitle\n"
        "                startDate\n"
        "                endDate\n"
        "            }\n"
        "        }\n"
        "        updatedDate\n"
        "    }\n"
        "}"
    )

    def __init__(self):
        super().__init__("catchplay")

    def get_catchplay_headers(self) -> dict:
        """Get headers required by CatchPlay GraphQL API"""
        return {
            "authorization": self.AUTHORIZATION,
            "content-type": "application/json",
            "asiaplay-device-type": "x",
            "user-agent": Config.DEFAULT_USER_AGENT,
        }

    @staticmethod
    def _mask_proxy(proxy_url: str) -> str:
        """Mask credentials in proxy URL for logging"""
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(proxy_url)
            if parsed.username or parsed.password:
                netloc = f"***:***@{parsed.hostname}"
                if parsed.port:
                    netloc += f":{parsed.port}"
                return urlunparse(parsed._replace(netloc=netloc))
            return proxy_url
        except Exception:
            return "***"

    def _request_channel_package(self) -> dict:
        """Blocking GraphQL request (runs inside asyncio.to_thread).

        Uses `requests` so the CatchPlay-specific proxy (incl. SOCKS via
        requests[socks]) can be applied, mirroring the Singtel approach.
        """
        import requests

        proxy_url = Config.get_catchplay_proxy()
        if proxy_url:
            self.logger.info(f"🔌 CatchPlay 使用代理: {self._mask_proxy(proxy_url)}")
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        body = {
            "operationName": "getChannelPackage",
            "query": self.GRAPHQL_QUERY,
            "variables": {},
        }

        response = requests.post(
            self.GRAPHQL_URL,
            headers=self.get_catchplay_headers(),
            json=body,
            proxies=proxies,
            timeout=Config.HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    async def _get_channel_package(self) -> List[dict]:
        """Fetch and validate the channel package, returning the raw channel list.

        Result is cached on the instance so fetch_channels + fetch_programs share
        a single network request.
        """
        data = await asyncio.to_thread(self._request_channel_package)

        # 地区限制错误：{"code": "core-0100", "message": "The location US is not allowed."}
        if isinstance(data, dict) and data.get("code"):
            raise RuntimeError(
                f"CatchPlay 接口返回错误 {data.get('code')}: {data.get('message')} "
                f"（该接口需要台湾 IP，请通过 CATCHPLAY_PROXY 配置台湾代理）"
            )

        channels = (
            (data or {}).get("data", {})
            .get("getChannelPackage", {})
            .get("channels")
        )
        if channels is None:
            raise RuntimeError(f"CatchPlay 接口响应异常: {str(data)[:200]}")

        return channels

    async def fetch_channels(self) -> List[Channel]:
        """Fetch all channels (with their embedded EPG) from CatchPlay"""
        self.logger.info("📡 正在从 CatchPlay 获取频道列表")

        raw_channels = await self._get_channel_package()

        channels = []
        for item in raw_channels:
            title = ((item.get("title") or {}).get("local") or "").strip()
            if not title:
                continue
            # 以频道名作为频道标识（与 4GTV 等平台保持一致，便于 tvg-name 匹配）
            channels.append(Channel(
                channel_id=title,
                name=title,
                epg=item.get("epg") or [],
            ))

        self.logger.info(f"📺 从 CatchPlay 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Parse EPG programs from the channels' embedded epg data (no extra request)"""
        tz_shanghai = ZoneInfo('Asia/Shanghai')
        all_programs = []

        for ch in channels:
            for entry in ch.extra_data.get("epg", []):
                try:
                    title = (entry.get("epgTitle") or "").strip()
                    start_raw = entry.get("startDate")
                    end_raw = entry.get("endDate")
                    if not title or not start_raw or not end_raw:
                        continue

                    start_utc = self._parse_iso_utc(start_raw)
                    end_utc = self._parse_iso_utc(end_raw)

                    all_programs.append(Program(
                        channel_id=ch.channel_id,
                        title=title,
                        start_time=start_utc.astimezone(tz_shanghai),
                        end_time=end_utc.astimezone(tz_shanghai),
                    ))
                except Exception as e:
                    self.logger.warning(f"⚠️ 解析 CatchPlay 节目数据失败: {e}")
                    continue

        self.logger.info(f"📊 总共抓取了 {len(all_programs)} 个 CatchPlay 节目")
        return all_programs

    @staticmethod
    def _parse_iso_utc(value: str) -> datetime:
        """Parse an ISO8601 UTC string (e.g. '2026-07-10T10:00:00Z') to aware UTC"""
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


# Create platform instance
catchplay_platform = CatchPlayPlatform()


# Legacy function for backward compatibility / main.py integration
async def get_catchplay_epg():
    """Fetch CatchPlay EPG data and return (channels, programs) in legacy dict format"""
    try:
        channels = await catchplay_platform.fetch_channels()
        if not channels:
            return [], []
        programs = await catchplay_platform.fetch_programs(channels)

        raw_channels = [
            {"channelName": channel.name, "channelId": channel.channel_id}
            for channel in channels
        ]

        raw_programs = [
            {
                "channelName": program.channel_id,
                "programName": program.title,
                "description": program.description,
                "start": program.start_time,
                "end": program.end_time,
            }
            for program in programs
        ]

        return raw_channels, raw_programs

    except Exception as e:
        logger.error(f"❌ get_catchplay_epg 函数错误: {e}", exc_info=True)
        return [], []
