import asyncio
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query

from .config import Config
from .logger import get_logger
from .file_manager import EPGFileManager
from .epg_platform import MyTvSuper, Hami
from .epg_platform.Astro import get_astro_epg
from .epg_platform.CN_epg_pw import get_cn_channels_epg
from .epg_platform.HOY import get_hoy_epg
from .epg_platform.NowTV import request_nowtv_today_epg
from .epg_platform.RTHK import get_rthk_epg
from .epg_platform.Starhub import get_starhub_epg
from .epg_platform.MeWatch import get_mewatch_epg
from .epg_platform.Singtel import get_singtel_epg
from .epg_platform.UnifiTV import get_unifitv_epg
from .epg_platform.FengShows import get_fengshows_epg
from .epg_platform.FourGTV import get_4gtv_epg
from .epg_platform.CatchPlay import get_catchplay_epg

logger = get_logger(__name__)

app = FastAPI(
    title=Config.APP_NAME,
    version=Config.APP_VERSION,
    description="Electronic Program Guide (EPG) aggregation service for Asian streaming platforms",
    openapi_url=None
)


@app.get("/")
async def root():
    """Health check endpoint"""
    enabled_platforms = [p["platform"] for p in Config.get_enabled_platforms()]
    return {
        "service": Config.APP_NAME,
        "version": Config.APP_VERSION,
        "status": "healthy",
        "enabled_platforms": enabled_platforms,
        "update_interval_minutes": Config.EPG_UPDATE_INTERVAL
    }


# Create scheduler instance
scheduler = AsyncIOScheduler()

# Guards against overlapping update runs (e.g. the startup task overlapping the
# first scheduled tick), which would otherwise double-crawl slow platforms.
_update_lock = asyncio.Lock()

# ===== /status caching =====
# EPG files change at most once per update cycle, so the status response is
# cached to keep the endpoint cheap even under heavy request volume:
#  - _status_response_cache: whole response, refreshed at most once per TTL
#  - _status_count_cache:    per-platform (channel/program) counts, reused while
#                            the file's path + mtime are unchanged (no re-parse)
_STATUS_CACHE_TTL = 30  # seconds
_status_response_cache = {"ts": 0.0, "data": None}
_status_count_cache = {}  # platform -> (path, mtime, channels, programs, parse_status)


def _epg_file_counts(platform: str):
    """Return (date_str, (channels, programs, parse_status)) for a platform's
    latest EPG file, parsing the XML only when the file actually changed."""
    date_str, path = EPGFileManager.get_latest_epg_file(platform)
    if not path or not os.path.exists(path):
        return date_str, None

    mtime = os.path.getmtime(path)
    cached = _status_count_cache.get(platform)
    if cached and cached[0] == path and cached[1] == mtime:
        return date_str, (cached[2], cached[3], cached[4])

    try:
        with open(path, "rb") as f:
            root = ET.fromstring(f.read())
        channels = len(root.findall("./channel"))
        programs = len(root.findall("./programme"))
        parse_status = "ok"
    except Exception:
        channels, programs, parse_status = 0, 0, "invalid"

    _status_count_cache[platform] = (path, mtime, channels, programs, parse_status)
    return date_str, (channels, programs, parse_status)


@scheduler.scheduled_job('interval', minutes=Config.EPG_UPDATE_INTERVAL)
async def scheduled_epg_update():
    """Scheduled task to update EPG data from all enabled platforms"""
    logger.info(f"🚀 开始定时更新EPG数据 - {datetime.now()}")
    await update_all_enabled_platforms()


async def request_my_tv_super_epg():
    """Update MyTV Super EPG data"""
    platform = "tvb"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await MyTvSuper.get_channels(force=True)
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_hami_epg():
    """Update Hami EPG data"""
    platform = "hami"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await Hami.request_all_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_cn_epg():
    """Update CN  EPG data"""
    platform = "cn"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        response_xml = await get_cn_channels_epg()
        if not response_xml:
            logger.warning(f"⚠️ 未收到{platform}的EPG数据")
            return

        # Convert string to bytes for consistent handling
        xml_bytes = response_xml.encode('utf-8') if isinstance(response_xml, str) else response_xml

        if EPGFileManager.save_epg_file(platform, xml_bytes):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_astro_epg():
    """Update Astro Go EPG data"""
    platform = "astro"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_astro_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_rthk_epg():
    """Update RTHK EPG data"""
    platform = "rthk"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_rthk_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_hoy_epg():
    """Update HOY EPG data"""
    platform = "hoy"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_hoy_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_now_tv_epg():
    """Update NowTV EPG data"""
    platform = "nowtv"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        response_xml = await request_nowtv_today_epg()
        if not response_xml:
            logger.warning(f"⚠️ 未收到{platform}的EPG数据")
            return

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_starhub_epg():
    """Update StarHub EPG data"""
    platform = "starhub"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_starhub_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_mewatch_epg():
    """Update MeWatch EPG data"""
    platform = "mewatch"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_mewatch_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_singtel_epg():
    """Update Singtel EPG data"""
    platform = "singtel"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_singtel_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_unifitv_epg():
    """Update UnifiTV EPG data"""
    platform = "unifitv"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_unifitv_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_fengshows_epg():
    """Update FengShows EPG data"""
    platform = "fengshows"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_fengshows_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_4gtv_epg():
    """Update 4GTV EPG data"""
    platform = "4gtv"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_4gtv_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


async def request_catchplay_epg():
    """Update CatchPlay EPG data"""
    platform = "catchplay"
    logger.info(f"📺 正在更新平台EPG数据: {platform}")

    try:
        if EPGFileManager.read_epg_file(platform) is not None:
            logger.info(f"✅ 今日{platform}的EPG数据已存在，跳过更新")
            return

        channels, programs = await get_catchplay_epg()
        if not channels:
            logger.warning(f"⚠️ 未找到{platform}的频道数据")
            return

        response_xml = await gen_channel(channels, programs)

        if EPGFileManager.save_epg_file(platform, response_xml):
            EPGFileManager.delete_old_epg_files(platform)
            logger.info(f"✨ 成功更新{platform}的EPG数据")
        else:
            logger.error(f"❌ 保存{platform}的EPG文件失败")

    except Exception as e:
        logger.error(f"💥 更新{platform}的EPG数据时发生错误: {e}", exc_info=True)


@app.get("/epg/{platform}")
async def get_platform_epg(platform: str):
    """Get EPG data for a specific platform"""
    logger.info(f"📡 提供平台EPG数据服务: {platform}")
    return EPGFileManager.get_single_platform_epg(platform)


@app.get("/epg")
async def get_custom_aggregate_epg(platforms: str = Query(..., description="Comma-separated platform list in priority order")):
    """
    Get aggregated EPG data from custom platform selection

    Example: ?platforms=tvb,nowtv,hami
    """
    platform_list = [p.strip() for p in platforms.split(',') if p.strip()]
    logger.info(f"📊 提供自定义聚合EPG数据服务: {platform_list}")
    return EPGFileManager.aggregate_epg_files(platform_list)


@app.get("/all")
async def get_all_enabled_platforms_epg():
    """Get aggregated EPG data from all enabled platforms (cached)"""
    logger.info(f"🌐 提供all平台的缓存EPG数据服务")
    return EPGFileManager.get_single_platform_epg("all")


@app.get("/all.xml.gz")
async def get_all_enabled_platforms_epg_gz():
    """Get aggregated EPG data from all enabled platforms (cached, gzip compressed)"""
    from fastapi.responses import FileResponse

    logger.info(f"📦 提供all平台的gz压缩缓存EPG数据服务")

    # Serve the newest available gz (matches the latest all xml) so the endpoint
    # keeps working during the daily refresh window instead of returning 404.
    date_str, xml_path = EPGFileManager.get_latest_epg_file("all")
    gz_file_path = xml_path.replace(".xml", ".xml.gz") if xml_path else None

    if not gz_file_path or not os.path.exists(gz_file_path):
        logger.error(f"❌ 未找到all.gz压缩文件: {gz_file_path}")
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail="Compressed EPG data not available. Please wait for next update cycle."
        )

    today = datetime.now().strftime('%Y%m%d')
    return FileResponse(
        path=gz_file_path,
        media_type="application/gzip",
        headers={
            "Content-Disposition": "attachment; filename=all.xml.gz",
            "Cache-Control": f"public, max-age={Config.EPG_CACHE_TTL}, s-maxage={Config.EPG_CACHE_TTL}",
            "ETag": f'"epg-all-gz-{date_str}"',
            "X-EPG-Date": date_str or "",
            "X-EPG-Stale": "true" if date_str != today else "false",
        },
        filename="all.xml.gz"
    )


@app.get("/status")
async def get_epg_status():
    """Report freshness of each platform's EPG data.

    For every configured platform (and the merged ``all`` cache) this returns the
    date its data is updated to, how many channels/programs that file contains,
    and whether it is current ("ok"), one-or-more days behind ("stale"),
    unparseable ("invalid") or absent ("missing") — so consumers can judge data
    quality at a glance.
    """
    # Serve a cached response if it is still fresh — bounds work to once per TTL
    # no matter how often the endpoint is hit. `updating` is overlaid live.
    now = time.monotonic()
    cached = _status_response_cache["data"]
    if cached is not None and (now - _status_response_cache["ts"]) < _STATUS_CACHE_TTL:
        return {**cached, "updating": _update_lock.locked(), "cached": True}

    today = datetime.now().strftime('%Y%m%d')
    enabled_set = {p["platform"] for p in Config.get_enabled_platforms()}

    entries = list(Config.EPG_PLATFORMS) + [{"platform": "all", "name": "All (merged)"}]

    platforms_info = []
    for conf in entries:
        platform = conf["platform"]
        date_str, counts = _epg_file_counts(platform)
        is_today = date_str == today

        info = {
            "platform": platform,
            "name": conf.get("name", platform),
            "enabled": platform == "all" or platform in enabled_set,
            "updated_to": date_str,
            "is_today": is_today,
            "channels": 0,
            "programs": 0,
            "status": "missing",
        }

        if counts is not None:
            channels, programs, parse_status = counts
            info["channels"] = channels
            info["programs"] = programs
            info["status"] = parse_status if parse_status == "invalid" else ("ok" if is_today else "stale")

        platforms_info.append(info)

    result = {
        "service": Config.APP_NAME,
        "version": Config.APP_VERSION,
        "today": today,
        "timezone": Config.EPG_TIMEZONE,
        "updating": _update_lock.locked(),
        "platforms": platforms_info,
        "cached": False,
    }
    _status_response_cache["ts"] = now
    _status_response_cache["data"] = result
    return result


async def gen_channel(channels, programs):
    """Generate EPG XML from channels and programs data"""
    from .epg.EpgGenerator import generateEpg
    return await generateEpg(channels, programs)


async def generate_all_platforms_cache():
    """Generate and cache merged EPG for all enabled platforms"""
    enabled_platforms = [p["platform"] for p in Config.get_enabled_platforms()]

    if not enabled_platforms:
        logger.warning("⚠️ 没有启用任何平台，无法生成all缓存")
        return

    logger.info(f"🔄 开始生成all平台合并缓存: {enabled_platforms}")

    try:
        import gzip

        merged_root = ET.Element("tv")
        merged_root.set("generator-info-name", f"{Config.APP_NAME} v{Config.APP_VERSION}")
        merged_root.set("generator-info-url", "https://github.com/CharmingCheung/CharmingEPG")

        channels_seen = set()
        total_channels = 0
        total_programs = 0

        for platform in enabled_platforms:
            # Use the latest available file (today's if present, else the most
            # recent) so the merged cache always contains every platform — slow
            # platforms simply contribute slightly stale data until they refresh.
            _date, content = EPGFileManager.read_latest_epg_file(platform)
            if not content:
                # Normal during the refresh window: platform still being fetched
                logger.info(f"⏳ 平台 {platform} 暂无可用数据（可能仍在抓取中），跳过合并")
                continue

            try:
                platform_root = ET.fromstring(content)

                # First pass: claim this platform's not-yet-seen channels
                # (first-come-first-served dedup across platforms)
                new_ids = set()
                for channel in platform_root.findall("./channel"):
                    channel_id = channel.get("id")
                    if channel_id and channel_id not in channels_seen:
                        channels_seen.add(channel_id)
                        new_ids.add(channel_id)
                        merged_root.append(channel)

                # Second pass: a single scan over programmes (O(channels + programmes)
                # instead of O(channels × programmes))
                platform_programs = 0
                for programme in platform_root.findall("./programme"):
                    if programme.get("channel") in new_ids:
                        merged_root.append(programme)
                        platform_programs += 1

                total_channels += len(new_ids)
                total_programs += platform_programs

                logger.debug(f"🔀 从{platform}合并{len(new_ids)}个频道和{platform_programs}个节目")

            except ET.ParseError as e:
                logger.error(f"❌ 解析平台{platform}的XML失败: {e}")
                continue

        if total_channels == 0:
            logger.error("❌ 任何平台都未找到有效的EPG数据，无法生成all缓存")
            return

        # Convert merged XML to bytes
        merged_xml = ET.tostring(merged_root, encoding="utf-8", xml_declaration=True)

        # Save to cache file using "all" as platform name
        if EPGFileManager.save_epg_file("all", merged_xml):
            logger.info(f"✨ 成功生成all缓存: {total_channels}个频道和{total_programs}个节目")
        else:
            logger.error("❌ 保存all缓存文件失败")
            return

        # Generate gzip compressed version
        compressed_xml = gzip.compress(merged_xml, compresslevel=9)
        gz_file_path = EPGFileManager.get_epg_file_path("all").replace(".xml", ".xml.gz")

        try:
            EPGFileManager.ensure_directory_exists(gz_file_path)
            tmp_gz_path = f"{gz_file_path}.tmp"
            with open(tmp_gz_path, "wb") as gz_file:
                gz_file.write(compressed_xml)
            os.replace(tmp_gz_path, gz_file_path)

            compression_ratio = len(compressed_xml) / len(merged_xml) * 100
            saved_ratio = 100 - compression_ratio
            logger.info(f"📦 成功生成all.gz压缩缓存: {len(compressed_xml)} 字节 (压缩至原来的 {compression_ratio:.1f}%，节省 {saved_ratio:.1f}%)")
        except Exception as gz_error:
            logger.error(f"❌ 保存all.gz压缩文件失败: {gz_error}")
            return

        # Delete old all EPG files (both .xml and .xml.gz)
        EPGFileManager.delete_old_epg_files("all")

        # Also delete old .gz files
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            current_gz_file = f"all_{current_date}.xml.gz"
            epg_dir = os.path.dirname(gz_file_path)

            deleted_gz_count = 0
            for file_name in os.listdir(epg_dir):
                if file_name.endswith(".xml.gz") and file_name != current_gz_file:
                    old_gz_path = os.path.join(epg_dir, file_name)
                    os.remove(old_gz_path)
                    deleted_gz_count += 1
                    logger.debug(f"🗑️ 删除旧压缩文件: {file_name}")

            if deleted_gz_count > 0:
                logger.info(f"🧹 清理all的{deleted_gz_count}个旧压缩文件")
        except Exception as cleanup_error:
            logger.error(f"❌ 清理旧压缩文件失败: {cleanup_error}")

    except Exception as e:
        logger.error(f"💥 生成all缓存时发生错误: {e}", exc_info=True)


async def update_all_enabled_platforms():
    """Update EPG data for all enabled platforms.

    Platforms are fetched concurrently. The merged ``all`` cache is regenerated
    incrementally as platforms finish (via ``as_completed``) instead of waiting
    for the slowest platform, so ``/all`` becomes available within seconds of the
    first platform completing and then refreshes as more data lands.
    """
    if _update_lock.locked():
        logger.info("⏳ 已有EPG更新任务在进行中，跳过本次触发")
        return

    async with _update_lock:
        enabled_platforms = Config.get_enabled_platforms()

        if not enabled_platforms:
            logger.warning("⚠️ 没有启用任何平台")
            return

        logger.info(f"🔄 开始更新{len(enabled_platforms)}个启用平台的EPG数据")

        # ===== 记录更新前各平台文件时间（仅看当天文件）=====
        mtime_before = {}
        for conf in enabled_platforms:
            path = EPGFileManager.get_epg_file_path(conf["platform"])
            mtime_before[conf["platform"]] = os.path.getmtime(path) if os.path.exists(path) else 0

        # today's "all" cache present? if not, the first completed platform forces a regen
        all_cache_exists = EPGFileManager.read_epg_file("all") is not None

        async def run_one(conf):
            """Run one platform's fetcher and report whether its file changed"""
            platform = conf["platform"]
            ok = True
            try:
                await globals()[conf["fetcher"]]()
            except Exception as e:
                ok = False
                logger.error(f"❌ 更新{conf['name']}的EPG数据失败: {e}", exc_info=True)

            path = EPGFileManager.get_epg_file_path(platform)
            changed = os.path.exists(path) and os.path.getmtime(path) > mtime_before.get(platform, 0)
            if changed:
                logger.info(f"🔁 检测到 {platform} 的EPG数据发生变化")
            return ok, changed

        tasks = [asyncio.create_task(run_one(conf)) for conf in enabled_platforms]

        success_count = 0
        error_count = 0
        need_initial_regen = not all_cache_exists

        for finished in asyncio.as_completed(tasks):
            ok, changed = await finished
            success_count += 1 if ok else 0
            error_count += 0 if ok else 1

            # Regenerate the merged cache as soon as there is something new to
            # publish (or the day's cache doesn't exist yet).
            if changed or need_initial_regen:
                need_initial_regen = False
                await generate_all_platforms_cache()

        logger.info(f"🎯 EPG数据更新完成: {success_count}个成功，{error_count}个失败")

        # Invalidate the /status cache so it reflects this cycle's data on next hit
        _status_response_cache["ts"] = 0.0
        _status_response_cache["data"] = None


@app.on_event("startup")
async def startup():
    """Application startup event"""
    logger.info(f"🚀 启动 {Config.APP_NAME} v{Config.APP_VERSION}")
    logger.info(f"⏰ EPG更新间隔: {Config.EPG_UPDATE_INTERVAL} 分钟")

    enabled_platforms = [p["name"] for p in Config.get_enabled_platforms()]
    logger.info(f"📺 已启用平台: {', '.join(enabled_platforms)}")

    # Start the scheduler
    scheduler.start()
    logger.info("⚡ 调度器已启动")

    # Trigger initial EPG update
    asyncio.create_task(update_all_enabled_platforms())
    logger.info("🎬 初始EPG数据更新已触发")