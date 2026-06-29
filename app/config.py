import os
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv(verbose=True, override=True)


class Config:
    """Centralized configuration management for CharmingEPG"""

    # Application settings
    APP_NAME = "CharmingEPG"
    APP_VERSION = "1.0.0"

    # Logging settings
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILE = os.getenv("LOG_FILE", "runtime.log")
    LOG_ROTATION = os.getenv("LOG_ROTATION", "10 MB")
    LOG_RETENTION = os.getenv("LOG_RETENTION", "7 days")

    # EPG settings
    EPG_UPDATE_INTERVAL = int(os.getenv("EPG_UPDATE_INTERVAL", "10"))  # minutes
    EPG_BASE_DIR = os.getenv("EPG_BASE_DIR", "epg_files")
    EPG_TIMEZONE = os.getenv("EPG_TIMEZONE", "Asia/Shanghai")
    EPG_CACHE_TTL = int(os.getenv("EPG_CACHE_TTL", "3600"))  # seconds (default: 1 hour)

    # HTTP client settings
    HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
    HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))
    HTTP_RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "2.0"))

    # Proxy settings
    PROXY_HTTP = os.getenv("PROXY_HTTP")
    PROXY_HTTPS = os.getenv("PROXY_HTTPS")

    # Singtel-specific proxy (Singtel API is geo-restricted to Singapore IPs)
    # Supports http://, https://, socks5://, socks5h:// URL schemes
    # Example: socks5://user:pass@host:1080 or http://host:8080
    SINGTEL_PROXY = os.getenv("SINGTEL_PROXY")

    @classmethod
    def get_proxies(cls) -> Optional[Dict[str, str]]:
        """Get proxy configuration if available"""
        if cls.PROXY_HTTP and cls.PROXY_HTTPS:
            return {
                "http": cls.PROXY_HTTP,
                "https": cls.PROXY_HTTPS
            }
        return None

    @classmethod
    def get_singtel_proxy(cls) -> Optional[str]:
        """Get Singtel-specific proxy URL (returns single URL string)"""
        proxy = (cls.SINGTEL_PROXY or "").strip()
        return proxy or None

    @classmethod
    def platform_enabled(cls, platform: str) -> bool:
        """Check if a platform is enabled via environment variable"""
        env_key = f"EPG_ENABLE_{platform.upper()}"
        val = os.getenv(env_key, "true").strip().lower()
        return val in {"1", "true", "yes", "on"}

    @classmethod
    def get_epg_file_path(cls, platform: str, date_str: str) -> str:
        """Get the file path for EPG data"""
        return os.path.join(cls.EPG_BASE_DIR, platform, f"{platform}_{date_str}.xml")

    # Platform configuration
    EPG_PLATFORMS = [
        {"platform": "tvb", "name": "MyTV Super", "fetcher": "request_my_tv_super_epg"},
        {"platform": "fengshows", "name": "FengShows", "fetcher": "request_fengshows_epg"},
        {"platform": "nowtv", "name": "NowTV", "fetcher": "request_now_tv_epg"},
        {"platform": "hami", "name": "Hami", "fetcher": "request_hami_epg"},
        {"platform": "astro", "name": "Astro Go", "fetcher": "request_astro_epg"},
        {"platform": "rthk", "name": "RTHK", "fetcher": "request_rthk_epg"},
        {"platform": "hoy", "name": "HOY", "fetcher": "request_hoy_epg"},
        {"platform": "starhub", "name": "StarHub", "fetcher": "request_starhub_epg"},
        {"platform": "mewatch", "name": "MeWatch", "fetcher": "request_mewatch_epg"},
        {"platform": "singtel", "name": "Singtel", "fetcher": "request_singtel_epg"},
        {"platform": "unifitv", "name": "UnifiTV", "fetcher": "request_unifitv_epg"},
        {"platform": "4gtv", "name": "4GTV", "fetcher": "request_4gtv_epg"},
        {"platform": "cn", "name": "CN", "fetcher": "request_cn_epg"},
    ]

    @classmethod
    def get_enabled_platforms(cls):
        """Get list of enabled platforms"""
        return [
            platform for platform in cls.EPG_PLATFORMS
            if cls.platform_enabled(platform["platform"])
        ]

    # User-Agent for HTTP requests
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    )