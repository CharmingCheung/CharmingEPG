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

# 按 <li> 逐条拆分解析，不要求属性在标签内固定顺序、兼容单双引号
RE_TABLIST = re.compile(r"""id=['"]TabList_Data['"]\s*>(.*)""", re.S)
RE_TABSPLIT = re.compile(r"""<div\s+class=['"]tab-TR(?:-vod)?['"]\s*>""")
RE_ENDTIME = re.compile(r"""endtime=['"](\d{3,4})['"]""")
RE_START = re.compile(r"""Time-message['"][^>]*>\s*(\d{1,2}:\d{2})""")
RE_H3 = re.compile(r"<h3[^>]*>(.*?)</h3>", re.S)
RE_TITLEAT = re.compile(r"""title=['"]([^'"]*)['"]""")
RE_TAGS = re.compile(r"<[^>]+>")


class _GaveUp(Exception):
    """连续撞 429 达到上限，放弃本次整轮抓取，避免落地不完整数据"""
    pass


class FourGTVPlatform(BaseEPGPlatform):
    """4GTV (四季線上) EPG platform implementation.

    频道列表来自 JSON API，节目表通过抓取每个频道页面的内嵌 HTML 解析。
    由于站点有 Cloudflare 防护与限速，抓取采用串行 + 随机延迟。
    节奏参数取自实测: 4.1s/个 + 每10个休息12s (约11.3请求/分钟)，
    116台频道背靠背连跑两轮共232个请求 0次429，约10分钟跑完。
    """

    CHANNEL_LIST_API = "https://api2.4gtv.tv/Channel/GetChannelBySetId/1/pc/L"
    CHANNEL_PAGE_URL = "https://www.4gtv.tv/channel/{fs_id}?set=1&ch={fn_id}"
    REFERER = "https://www.4gtv.tv/"

    # 抓取节奏（实测验证档，见类 docstring）
    SPACING = 4.1                 # 频道间隔基准值 (秒)
    BATCH_SIZE = 10                # 每 N 个频道休息一次
    BATCH_PAUSE = 12.0             # 批间休息基准值 (秒)
    SAFE_SPACING = 6.3             # 撞 429 后回落的已验证安全档
    SAFE_PAUSE = 19.0
    JITTER_SPACING = (0.85, 1.15)
    JITTER_PAUSE = (0.90, 1.10)
    BACKOFF_429 = 600              # 撞 429 退避 (秒)
    SLOWDOWN_429 = 1.60            # 已在安全档之上时再放慢的倍数
    MAX_429 = 2                    # 累计撞满这么多次 429 就放弃本次抓取
    PENALTY_MERGE = 20.0           # 同一次限速事件的去重窗口
    MAX_RETRIES = 3
    PROGRESS_EVERY = 10           # 每 N 个频道打印一次进度心跳

    def __init__(self):
        super().__init__("4gtv")
        self._spacing = self.SPACING
        self._pause = self.BATCH_PAUSE
        self._n_429 = 0
        self._n_403 = 0
        self._penalty_until = 0.0
        self._session = None

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
        total = len(channels)

        # 每轮抓取重置节奏与限速状态
        self._spacing = self.SPACING
        self._pause = self.BATCH_PAUSE
        self._n_429 = 0
        self._n_403 = 0
        self._penalty_until = 0.0
        self._session = None

        # 粗略预估耗时：每频道间隔 + 批间休息，让用户对总时长有预期
        est_minutes = (total * self._spacing + max(0, (total - 1) // self.BATCH_SIZE) * self._pause) / 60
        self.logger.info(
            f"📡 正在抓取 4GTV EPG 数据 "
            f"(频道: {total}, 基准日期: {today.strftime('%Y-%m-%d')}, 今天/明天/後天)；"
            f"串行+限速抓取，预计耗时约 {est_minutes:.0f} 分钟"
        )

        all_programs = []
        success = 0
        fail = 0

        try:
            for i, ch in enumerate(channels):
                # 每 BATCH_SIZE 个频道休息一次 (跳过第一个)
                if i > 0 and i % self.BATCH_SIZE == 0:
                    pause = self._pause * random.uniform(*self.JITTER_PAUSE)
                    self.logger.info(
                        f"⏸️ 4GTV 批次休息 {pause:.0f}s... "
                        f"(进度 {i}/{total}, 成功 {success}, 累计 {len(all_programs)} 节目)"
                    )
                    await asyncio.sleep(pause)

                self.logger.debug(f"🔄【4GTV】抓取频道 {i + 1}/{total}: {ch.name}")

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

                # 进度心跳：让用户清楚地看到抓取仍在进行
                if (i + 1) % self.PROGRESS_EVERY == 0 or (i + 1) == total:
                    self.logger.info(
                        f"📈 4GTV 抓取进度 {i + 1}/{total} "
                        f"(成功 {success}, 失败/无数据 {fail}, 累计 {len(all_programs)} 节目)"
                    )

                await asyncio.sleep(self._spacing * random.uniform(*self.JITTER_SPACING))
        except _GaveUp:
            self.logger.error("❌ 4GTV 连续撞 429 达到上限，放弃本次抓取")
            raise

        self.logger.info(
            f"📊 总共抓取了 {len(all_programs)} 个 4GTV 节目 "
            f"(成功: {success}, 失败/无数据: {fail})"
        )
        return all_programs

    def _get_session(self, cffi_requests):
        """获取复用的 curl_cffi Session，首次创建时预热访问首页拿 CF cookie。

        整个抓取过程严格顺序执行 (同一时刻只有一个 to_thread 调用在跑)，
        因此单实例属性即可，无需线程本地存储。
        """
        if self._session is not None:
            return self._session

        session = cffi_requests.Session()
        try:
            session.get(
                self.REFERER,
                headers={"User-Agent": self._get_ua()},
                timeout=Config.HTTP_TIMEOUT,
                verify=False,
                impersonate="chrome120",
            )
        except Exception:
            pass  # 预热失败不影响后续正式请求
        self._session = session
        return session

    def _on_429(self):
        """撞 429：退避并回落节奏，累计达上限则放弃本次整轮抓取"""
        self._n_429 += 1
        now = time.time()
        if now < self._penalty_until:
            return
        self._penalty_until = now + self.PENALTY_MERGE

        if self._n_429 >= self.MAX_429:
            self.logger.error(f"❌ 4GTV 已撞 {self._n_429} 次 429，放弃本次抓取")
            raise _GaveUp(f"连续 {self._n_429} 次 429 限速")

        if self._spacing < self.SAFE_SPACING:
            self._spacing, self._pause = self.SAFE_SPACING, self.SAFE_PAUSE
        else:
            self._spacing *= self.SLOWDOWN_429
            self._pause *= self.SLOWDOWN_429

        rate_per_min = 60.0 / (self._spacing + self._pause / self.BATCH_SIZE)
        self.logger.warning(
            f"⚠️ 4GTV 429 限速! 退避 {self.BACKOFF_429 // 60} 分钟, "
            f"之后放慢到 {self._spacing:.1f}s/个 ({rate_per_min:.1f} 请求/分钟)"
        )
        time.sleep(self.BACKOFF_429)

    def _on_403(self):
        """撞 403 (CF 拦截)：退避、放慢节奏、丢弃 session 强制下次重建+重新预热"""
        self._n_403 += 1
        now = time.time()
        if now < self._penalty_until:
            return
        self._penalty_until = now + self.PENALTY_MERGE
        self._spacing *= 1.3
        self._session = None
        self.logger.warning(f"⚠️ 4GTV 403 CF拦截, 退避 60s, 放慢到 {self._spacing:.1f}s/个")
        time.sleep(60)

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
            try:
                r = self._get_session(cffi_requests).get(
                    url,
                    headers={
                        "User-Agent": self._get_ua(),
                        "Referer": self.REFERER,
                        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                    },
                    timeout=Config.HTTP_TIMEOUT,
                    verify=False,
                    impersonate="chrome120",
                )

                if r.status_code == 200:
                    return r.text

                if r.status_code == 429:
                    self._on_429()
                    continue

                if r.status_code == 403:
                    self._on_403()
                    continue

                self.logger.error(f"❌ 4GTV HTTP {r.status_code}: {url}")
                return ""

            except _GaveUp:
                raise
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    self.logger.warning(f"⚠️ 4GTV 请求异常 {type(e).__name__}, 重试 {attempt + 1}")
                    time.sleep(5 * (attempt + 1))
                else:
                    self.logger.error(f"❌ 4GTV 请求失败: {e}")
                    return ""
        return ""

    def _parse_epg_from_html(self, html: str, base_date: datetime, channel_id: str) -> List[Program]:
        """Parse the EPG embedded in a channel page (today / tomorrow / day-after).

        按每个 <li> 逐条拆分再分别提取字段，不要求属性在标签内固定顺序，
        兼容单/双引号 —— 比大正则整体匹配更能抗 HTML 属性顺序变化。
        """
        m = RE_TABLIST.search(html or "")
        if not m:
            return []

        blocks = [b for b in RE_TABSPLIT.split(m.group(1)) if "<li" in b]

        programs = []
        for day_offset, block in enumerate(blocks[:3]):
            current_date = base_date + timedelta(days=day_offset)

            for li in re.split(r"<li\b", block)[1:]:
                m_end = RE_ENDTIME.search(li)
                m_start = RE_START.search(li)
                if not m_end or not m_start:
                    continue

                m_h3 = RE_H3.search(li)
                title = RE_TAGS.sub("", m_h3.group(1)).strip() if m_h3 else ""
                if not title:
                    m_at = RE_TITLEAT.search(li)
                    title = m_at.group(1).strip() if m_at else ""
                if not title:
                    continue

                end_raw = m_end.group(1).zfill(4)
                sh, sm = (int(x) for x in m_start.group(1).split(":"))
                eh, em = int(end_raw[:2]), int(end_raw[2:4])
                if sh > 23 or eh > 23 or sm > 59 or em > 59:
                    continue

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
