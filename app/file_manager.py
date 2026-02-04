import os
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional
from fastapi import HTTPException
from fastapi.responses import Response

from .config import Config
from .logger import get_logger

logger = get_logger(__name__)


class EPGFileManager:
    """Manages EPG file operations including reading, writing, and aggregation"""

    @staticmethod
    def get_epg_file_path(platform: str, date_str: str = None) -> str:
        """Get the file path for EPG data"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
        return Config.get_epg_file_path(platform, date_str)

    @staticmethod
    def ensure_directory_exists(file_path: str):
        """Create directory if it doesn't exist"""
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            logger.info(f"📌 创建目录: {directory}")

    @staticmethod
    def read_epg_file(platform: str, date_str: str = None) -> Optional[bytes]:
        """
        Read EPG file content for a platform

        Args:
            platform: Platform name
            date_str: Date string (YYYYMMDD), defaults to today

        Returns:
            File content as bytes or None if file doesn't exist
        """
        file_path = EPGFileManager.get_epg_file_path(platform, date_str)

        if not os.path.exists(file_path):
            logger.warning(f"⚠️ EPG文件未找到: {file_path}")
            return None

        try:
            with open(file_path, "rb") as file:
                content = file.read()
                logger.debug(f"✅ 成功读取EPG文件: {file_path} ({len(content)} 字节)")
                return content
        except Exception as e:
            logger.error(f"❌ 读取EPG文件失败 {file_path}: {e}")
            return None

    @staticmethod
    def save_epg_file(platform: str, content: bytes, date_str: str = None) -> bool:
        """
        Save EPG content to file

        Args:
            platform: Platform name
            content: XML content as bytes
            date_str: Date string (YYYYMMDD), defaults to today

        Returns:
            True if successful, False otherwise
        """
        file_path = EPGFileManager.get_epg_file_path(platform, date_str)

        try:
            EPGFileManager.ensure_directory_exists(file_path)

            with open(file_path, "wb") as file:
                file.write(content)

            logger.info(f"💾 保存EPG文件: {file_path} ({len(content)} 字节)")
            return True

        except Exception as e:
            logger.error(f"❌ 保存EPG文件失败 {file_path}: {e}")
            return False

    @staticmethod
    def delete_old_epg_files(platform: str, keep_current: bool = True) -> int:
        """
        Delete old EPG files for a platform

        Args:
            platform: Platform name
            keep_current: Whether to keep today's file

        Returns:
            Number of files deleted
        """
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            current_file = f"{platform}_{current_date}.xml"
            epg_dir = os.path.dirname(EPGFileManager.get_epg_file_path(platform))

            if not os.path.exists(epg_dir):
                return 0

            deleted_count = 0
            for file_name in os.listdir(epg_dir):
                if file_name.endswith(".xml"):
                    if not keep_current or file_name != current_file:
                        file_path = os.path.join(epg_dir, file_name)
                        os.remove(file_path)
                        deleted_count += 1
                        logger.debug(f"🗑️ 删除旧EPG文件: {file_name}")

            if deleted_count > 0:
                logger.info(f"🧹 清理{platform}的{deleted_count}个旧EPG文件")

            return deleted_count

        except Exception as e:
            logger.error(f"❌ 删除{platform}的旧EPG文件失败: {e}")
            return 0

    @staticmethod
    def aggregate_epg_files(platforms: List[str]) -> Response:
        """
        Aggregate EPG files from multiple platforms

        Args:
            platforms: List of platform names to aggregate

        Returns:
            FastAPI Response with aggregated XML content

        Raises:
            HTTPException: If no EPG data is available
        """
        logger.info(f"🔄 正在聚合平台EPG数据: {platforms}")

        merged_root = ET.Element("tv")
        merged_root.set("generator-info-name", f"{Config.APP_NAME} v{Config.APP_VERSION}")
        merged_root.set("generator-info-url", "https://github.com/CharmingCheung/CharmingEPG")

        channels_seen = set()
        total_channels = 0
        total_programs = 0

        for platform in platforms:
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
                        for programme in platform_root.findall(f"./programme[@channel='{channel_id}']"):
                            merged_root.append(programme)
                            platform_programs += 1

                total_channels += platform_channels
                total_programs += platform_programs

                logger.debug(
                    f"🔀 从{platform}合并{platform_channels}个频道和{platform_programs}个节目"
                )

            except ET.ParseError as e:
                logger.error(f"❌ 解析平台{platform}的XML失败: {e}")
                continue

        if total_channels == 0:
            logger.error("❌ 任何平台都未找到有效的EPG数据")
            raise HTTPException(status_code=404, detail="No EPG data available")

        # Convert merged XML to string
        merged_xml = ET.tostring(merged_root, encoding="utf-8", xml_declaration=True)

        logger.info(f"✨ 成功聚合{total_channels}个频道和{total_programs}个节目")

        return Response(
            content=merged_xml,
            media_type="application/xml",
            headers={
                "Content-Disposition": "attachment; filename=epg.xml",
                "Cache-Control": f"public, max-age={Config.EPG_CACHE_TTL}, s-maxage={Config.EPG_CACHE_TTL}",
                "ETag": f'"epg-{datetime.now().strftime("%Y%m%d")}-{",".join(platforms)}"',
                "X-Total-Channels": str(total_channels),
                "X-Total-Programs": str(total_programs),
                "X-Platforms": ",".join(platforms)
            }
        )

    @staticmethod
    def get_single_platform_epg(platform: str) -> Response:
        """
        Get EPG data for a single platform

        Args:
            platform: Platform name

        Returns:
            FastAPI Response with XML content

        Raises:
            HTTPException: If EPG file is not found
        """
        content = EPGFileManager.read_epg_file(platform)

        if content is None:
            logger.error(f"❌ 未找到平台{platform}的EPG文件")
            raise HTTPException(
                status_code=404,
                detail=f"EPG data not available for platform: {platform}"
            )

        # Parse XML to get channel and program counts for headers
        try:
            root = ET.fromstring(content)
            channel_count = len(root.findall("./channel"))
            program_count = len(root.findall("./programme"))

            logger.info(f"📡 为{platform}提供EPG服务: {channel_count}个频道，{program_count}个节目")

            return Response(
                content=content,
                media_type="application/xml",
                headers={
                    "Content-Disposition": f"attachment; filename={platform}_epg.xml",
                    "Cache-Control": f"public, max-age={Config.EPG_CACHE_TTL}, s-maxage={Config.EPG_CACHE_TTL}",
                    "ETag": f'"epg-{platform}-{datetime.now().strftime("%Y%m%d")}"',
                    "X-Platform": platform,
                    "X-Total-Channels": str(channel_count),
                    "X-Total-Programs": str(program_count)
                }
            )

        except ET.ParseError:
            logger.warning(f"⚠️ 平台{platform}的XML内容无效，按原样提供服务")
            return Response(
                content=content,
                media_type="application/xml",
                headers={
                    "Content-Disposition": f"attachment; filename={platform}_epg.xml",
                    "Cache-Control": f"public, max-age={Config.EPG_CACHE_TTL}, s-maxage={Config.EPG_CACHE_TTL}",
                    "ETag": f'"epg-{platform}-{datetime.now().strftime("%Y%m%d")}"',
                    "X-Platform": platform
                }
            )