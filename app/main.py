import asyncio
import os
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
    """Update CN (epg.pw) EPG data"""
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
    import os

    logger.info(f"📦 提供all平台的gz压缩缓存EPG数据服务")

    gz_file_path = EPGFileManager.get_epg_file_path("all").replace(".xml", ".xml.gz")

    if not os.path.exists(gz_file_path):
        logger.error(f"❌ 未找到all.gz压缩文件: {gz_file_path}")
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail="Compressed EPG data not available. Please wait for next update cycle."
        )

    return FileResponse(
        path=gz_file_path,
        media_type="application/gzip",
        headers={
            "Content-Disposition": "attachment; filename=all.xml.gz",
            "Cache-Control": f"public, max-age={Config.EPG_CACHE_TTL}, s-maxage={Config.EPG_CACHE_TTL}",
            "ETag": f'"epg-all-gz-{datetime.now().strftime("%Y%m%d")}"'
        },
        filename="all.xml.gz"
    )


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
        # Use existing aggregate logic to merge all platforms
        import xml.etree.ElementTree as ET
        import gzip

        merged_root = ET.Element("tv")
        merged_root.set("generator-info-name", f"{Config.APP_NAME} v{Config.APP_VERSION}")
        merged_root.set("generator-info-url", "https://github.com/CharmingCheung/CharmingEPG")

        channels_seen = set()
        total_channels = 0
        total_programs = 0

        for platform in enabled_platforms:
            content = EPGFileManager.read_epg_file(platform)
            if not content:
                logger.warning(f"⚠️ 未找到平台的EPG数据: {platform}")
                continue

            try:
                platform_root = ET.fromstring(content)

                # Process channels (first-come-first-served for duplicates)
                platform_channels = 0
                platform_programs = 0

                for channel in platform_root.findall("./channel"):
                    channel_id = channel.get("id")
                    if channel_id and channel_id not in channels_seen:
                        channels_seen.add(channel_id)
                        merged_root.append(channel)
                        platform_channels += 1

                        # Add all programs for this channel
                        # Use manual filtering instead of XPath to avoid issues with special characters
                        for programme in platform_root.findall("./programme"):
                            if programme.get("channel") == channel_id:
                                merged_root.append(programme)
                                platform_programs += 1

                total_channels += platform_channels
                total_programs += platform_programs

                logger.debug(f"🔀 从{platform}合并{platform_channels}个频道和{platform_programs}个节目")

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
            with open(gz_file_path, "wb") as gz_file:
                gz_file.write(compressed_xml)

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
    """Update EPG data for all enabled platforms"""
    enabled_platforms = Config.get_enabled_platforms()

    if not enabled_platforms:
        logger.warning("⚠️ 没有启用任何平台")
        return

    logger.info(f"🔄 开始更新{len(enabled_platforms)}个启用平台的EPG数据")

    # ===== 记录更新前各平台文件时间 =====
    platform_mtime_before = {}
    for conf in enabled_platforms:
        platform = conf["platform"]
        path = EPGFileManager.get_epg_file_path(platform)
        platform_mtime_before[platform] = os.path.getmtime(path) if os.path.exists(path) else 0

    # ===== 并发执行更新 =====
    tasks = [globals()[conf["fetcher"]]() for conf in enabled_platforms]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # ===== 统计结果 =====
    success_count = 0
    error_count = 0
    any_updated = False

    for i, result in enumerate(results):
        platform_conf = enabled_platforms[i]
        platform = platform_conf["platform"]
        platform_name = platform_conf["name"]

        if isinstance(result, Exception):
            error_count += 1
            logger.error(f"❌ 更新{platform_name}的EPG数据失败: {result}", exc_info=True)
            continue

        success_count += 1

        # 对比文件更新时间
        path = EPGFileManager.get_epg_file_path(platform)
        if os.path.exists(path):
            if os.path.getmtime(path) > platform_mtime_before.get(platform, 0):
                any_updated = True
                logger.info(f"🔁 检测到 {platform} 的EPG数据发生变化")

    logger.info(f"🎯 EPG数据更新完成: {success_count}个成功，{error_count}个失败")

    # ===== 决定是否生成 all =====
    all_cache_exists = EPGFileManager.read_epg_file("all") is not None

    if not all_cache_exists or any_updated:
        logger.info("📝 需要重新生成all缓存")
        await generate_all_platforms_cache()
    else:
        logger.info("✅ all缓存已存在且本轮无平台更新，跳过重新生成")


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