import asyncio
import unittest
from unittest.mock import Mock, patch

from curl_cffi import CurlHttpVersion

from app.epg_platform.FourGTV import FourGTVPlatform
from app.config import Config


class FourGTVTLSTest(unittest.TestCase):
    def test_channel_api_verifies_certificate(self):
        with patch("curl_cffi.requests.get") as get:
            get.return_value.json.return_value = {"Data": []}
            asyncio.run(FourGTVPlatform().fetch_channels())
        self.assertIs(get.call_args.kwargs["verify"], True)
        self.assertEqual(get.call_args.kwargs["http_version"], CurlHttpVersion.V1_1)

    def test_warmup_and_channel_page_verify_certificate(self):
        session = Mock()
        session.get.return_value.status_code = 200
        session.get.return_value.text = "schedule"
        with patch("curl_cffi.requests.Session", return_value=session) as create_session:
            content = FourGTVPlatform()._fetch_url("https://www.4gtv.tv/channel/test")
        create_session.assert_called_once_with(
            http_version=CurlHttpVersion.V1_1,
            proxies=FourGTVPlatform._get_proxies(),
        )
        self.assertEqual(content, "schedule")
        self.assertEqual(session.get.call_count, 2)
        for call in session.get.call_args_list:
            self.assertIs(call.kwargs["verify"], True)
        user_agents = [call.kwargs['headers']['User-Agent'] for call in session.get.call_args_list]
        self.assertEqual(len(set(user_agents)), 1)
        self.assertIn('Chrome/120.', user_agents[0])

    def test_proxy_routes_channel_list_and_page_session(self):
        proxy = 'socks5h://127.0.0.1:1080'
        expected = {'http': proxy, 'https': proxy}
        session = Mock()
        session.get.return_value.status_code = 200
        with patch.object(Config, 'FOURGTV_PROXY', proxy), patch(
            'curl_cffi.requests.get'
        ) as get, patch('curl_cffi.requests.Session', return_value=session) as create:
            get.return_value.json.return_value = {'Data': []}
            platform = FourGTVPlatform()
            asyncio.run(platform.fetch_channels())
            platform._fetch_url('https://www.4gtv.tv/channel/test')
        self.assertEqual(get.call_args.kwargs['proxies'], expected)
        self.assertEqual(create.call_args.kwargs['proxies'], expected)

    def test_global_proxy_fallback_and_session_cleanup(self):
        expected = {'http': 'http://127.0.0.1:8080', 'https': 'http://127.0.0.1:8080'}
        platform = FourGTVPlatform()
        session = Mock()
        platform._session = session
        with patch.object(Config, 'FOURGTV_PROXY', None), patch.object(
            Config, 'get_proxies', return_value=expected
        ), patch('app.epg_platform.FourGTV.time.sleep'):
            self.assertEqual(platform._get_proxies(), expected)
            platform._on_403()
        session.close.assert_called_once()
        self.assertIsNone(platform._session)

    def test_telus_disables_http2(self):
        from app.epg_platform.Telus import TelusPlatform

        with patch("curl_cffi.requests.Session") as create_session:
            TelusPlatform()._get_session()
        create_session.assert_called_once_with(http_version=CurlHttpVersion.V1_1)
