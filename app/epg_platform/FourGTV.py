import asyncio
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import List

from ..logger import get_logger
from ..config import Config
from .base import BaseEPGPlatform, Channel, Program

logger = get_logger(__name__)

# 四季線上 4gTV 使用 Cloudflare 防护，需要 curl_cffi 的浏览器指纹模拟才能正常访问。
# 节目表内嵌在频道页 HTML 中（今天 / 明天 / 後天，共 3 天）。

TW = timezone(timedelta(hours=8))  # UTC+8 (台湾时间，与 Asia/Shanghai 偏移一致)


class FourGTVPlatform(BaseEPGPlatform):
    """4GTV (四季線上) EPG platform implementation.

    频道列表来自 JSON API，节目表通过抓取每个频道页面的内嵌 HTML 解析。
    由于站点有 Cloudflare 防护与限速，抓取采用串行 + 随机延迟，整体较慢。
    """

    CHANNEL_LIST_API = "https://api2.4gtv.tv/Channel/GetChannelBySetId/1/pc/L"
    CHANNEL_PAGE_URL = "https://www.4gtv.tv/channel/{fs_id}?set=1&ch={fn_id}"
    REFERER = "https://www.4gtv.tv/"

    # 抓取节奏（保守设置，避免触发 Cloudflare 限速）
    CHANNEL_DELAY = (6.0, 10.0)   # 频道间随机延迟 (秒)
    BATCH_SIZE = 10               # 每 N 个频道休息一次
    BATCH_PAUSE = 60              # 批间休息 (秒)
    RATE_LIMIT_WAIT = 90          # 429 限速等待 (秒)
    MAX_RETRIES = 3

    def __init__(self):
        super().__init__("4gtv")
        self._rate_limit_until = 0.0

    @staticmethod
    def _get_ua() -> str:
        v = random.choice(["120.0.0.0", "121.0.0.0", "122.0.0.0", "123.0.0.0", "124.0.0.0"])
        return (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v} Safari/537.36")

    async def fetch_channels(self) -> List[Channel]:
        """Fetch the channel list from the 4GTV JSON API"""
        self.logger.info("📡 正在从 4GTV 获取频道列表")

        try:
            from curl_cffi import requests as cffi_requests
        except ImportError:
            self.logger.error("❌ 缺少依赖 curl_cffi，无法抓取 4GTV，请执行 pip install curl_cffi")
            return []

        def _request():
            r = cffi_requests.get(
                self.CHANNEL_LIST_API,
                headers={"User-Agent": self._get_ua()},
                timeout=Config.HTTP_TIMEOUT,
                verify=False,
                impersonate="chrome120",
            )
            return r.json()

        try:
            data = await asyncio.to_thread(_request)
        except Exception as e:
            self.logger.error(f"❌ 4GTV 频道列表请求失败: {e}")
            return []

        if not data or "Data" not in data:
            self.logger.warning("⚠️ 4GTV 频道列表响应异常")
            return []

        channels = []
        for item in data["Data"]:
            fn_id = item.get("fnID")
            fs_id = item.get("fs4GTV_ID", "")
            name = (item.get("fsNAME", "") or "").strip()
            logo = item.get("fsLOGO_MOBILE", "")
            group = item.get("fsTYPE_NAME", "未分类")
            if not fn_id or not fs_id or not name:
                continue
            groups = re.split(r"[,，、]", group)
            group = groups[0].strip() if groups and groups[0].strip() else "未分类"
            channels.append(Channel(
                channel_id=name,
                name=name,
                fn_id=str(fn_id),
                fs_id=fs_id,
                logo=logo,
                group=group,
            ))

        self.logger.info(f"📺 从 4GTV 发现 {len(channels)} 个频道")
        return channels

    async def fetch_programs(self, channels: List[Channel]) -> List[Program]:
        """Fetch EPG data by scraping each channel page (serial with rate limiting)"""
        today = datetime.now(TW).replace(hour=0, minute=0, second=0, microsecond=0)
        self.logger.info(
            f"📡 正在抓取 4GTV EPG 数据 "
            f"(频道: {len(channels)}, 基准日期: {today.strftime('%Y-%m-%d')}, 今天/明天/後天)"
        )

        all_programs = []
        success = 0
        fail = 0

        for i, ch in enumerate(channels):
            # 每 BATCH_SIZE 个频道休息一次 (跳过第一个)
            if i > 0 and i % self.BATCH_SIZE == 0:
                self.logger.info(f"⏸️ 4GTV 批次休息 {self.BATCH_PAUSE}s... (进度 {i}/{len(channels)})")
                await asyncio.sleep(self.BATCH_PAUSE)

            url = self.CHANNEL_PAGE_URL.format(fs_id=ch.extra_data["fs_id"], fn_id=ch.extra_data["fn_id"])
            html = await asyncio.to_thread(self._fetch_url, url)

            if html:
                progs = self._parse_epg_from_html(html, today, ch.channel_id)
                if progs:
                    all_programs.extend(progs)
                    success += 1
                else:
                    fail += 1
                    self.logger.warning(f"⚠️ 4GTV 无节目数据: {ch.name}")
            else:
                fail += 1

            await asyncio.sleep(random.uniform(*self.CHANNEL_DELAY))

        self.logger.info(
            f"📊 总共抓取了 {len(all_programs)} 个 4GTV 节目 "
            f"(成功: {success}, 失败/无数据: {fail})"
        )
        return all_programs

    def _fetch_url(self, url: str) -> str:
        """Blocking fetch with Cloudflare impersonation, 429/403 retries.

        Runs inside a thread (asyncio.to_thread); uses time.sleep for backoff.
        """
        try:
            from curl_cffi import requests as cffi_requests
        except ImportError:
            self.logger.error("❌ 缺少依赖 curl_cffi，无法抓取 4GTV，请执行 pip install curl_cffi")
            return ""

        for attempt in range(self.MAX_RETRIES):
            now = time.time()
            if self._rate_limit_until > now:
                time.sleep(self._rate_limit_until - now)

            try:
                r = cffi_requests.get(
                    url,
                    headers={
                        "User-Agent": self._get_ua(),
                        "Referer": self.REFERER,
                        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                    },
                    timeout=Config.HTTP_TIMEOUT,
                    verify=False,
                    impersonate=random.choice(["chrome120", "chrome110"]),
                )

                if r.status_code == 200:
                    return r.text

                if r.status_code == 429:
                    wait = self.RATE_LIMIT_WAIT * (attempt + 1)
                    self.logger.warning(f"⚠️ 4GTV 429 限速! 等待 {wait}s ({attempt + 1}/{self.MAX_RETRIES})")
                    self._rate_limit_until = time.time() + wait
                    time.sleep(wait)
                    continue

                if r.status_code == 403 and attempt < self.MAX_RETRIES - 1:
                    self.logger.warning(f"⚠️ 4GTV 403 CF拦截, 重试 {attempt + 1}")
                    time.sleep(10 * (attempt + 1))
                    continue

                self.logger.error(f"❌ 4GTV HTTP {r.status_code}: {url}")
                return ""

            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    self.logger.warning(f"⚠️ 4GTV 请求异常 {type(e).__name__}, 重试 {attempt + 1}")
                    time.sleep(5 * (attempt + 1))
                else:
                    self.logger.error(f"❌ 4GTV 请求失败: {e}")
                    return ""
        return ""

    def _parse_epg_from_html(self, html: str, base_date: datetime, channel_id: str) -> List[Program]:
        """Parse the EPG embedded in a channel page (today / tomorrow / day-after)"""
        m = re.search(r'id="TabList_Data">(.*)', html, re.S)
        if not m:
            return []
        data_block = m.group(1)

        tab_blocks = re.split(r"<div\s+class='tab-TR(?:-vod)?'>", data_block)
        tab_blocks = [b for b in tab_blocks if '<li' in b]

        programs = []
        for day_offset, block in enumerate(tab_blocks):
            if day_offset >= 3:
                break
            current_date = base_date + timedelta(days=day_offset)

            items = re.findall(
                r"<li\s+[^>]*title='([^']*)'[^>]*>.*?"
                r"<div\s+class='Time-message'\s+endtime='(\d{4})'>(\d{2}:\d{2})</div>.*?"
                r"<h3>(.*?)</h3>",
                block, re.S
            )

            for title_attr, endtime_raw, starttime_raw, h3_raw in items:
                title = re.sub(r'<[^>]+>', '', h3_raw).strip() or title_attr.strip()
                if not title:
                    continue

                sh, sm = int(starttime_raw[:2]), int(starttime_raw[3:5])
                eh, em = int(endtime_raw[:2]), int(endtime_raw[2:4])

                start_dt = current_date.replace(hour=sh, minute=sm)
                end_dt = current_date.replace(hour=eh, minute=em)
                # 结束时间不大于开始时间则跨日
                if (eh * 60 + em) <= (sh * 60 + sm):
                    end_dt += timedelta(days=1)

                programs.append(Program(
                    channel_id=channel_id,
                    title=title,
                    start_time=start_dt,
                    end_time=end_dt,
                ))

        # 去重 (title, start)
        seen = set()
        unique = []
        for p in programs:
            key = (p.title, p.start_time)
            if key not in seen:
                seen.add(key)
                unique.append(p)
        return unique


# Create platform instance
fourgtv_platform = FourGTVPlatform()


# Legacy function for backward compatibility / main.py integration
async def get_4gtv_epg():
    """Fetch 4GTV EPG data and return (channels, programs) in legacy dict format"""
    try:
        channels = await fourgtv_platform.fetch_channels()
        if not channels:
            return [], []
        programs = await fourgtv_platform.fetch_programs(channels)

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
        logger.error(f"❌ get_4gtv_epg 函数错误: {e}", exc_info=True)
        return [], []
