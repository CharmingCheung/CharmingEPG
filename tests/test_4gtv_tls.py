import asyncio
import unittest
from unittest.mock import Mock, patch

from curl_cffi import CurlHttpVersion

from app.epg_platform.FourGTV import FourGTVPlatform


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
        create_session.assert_called_once_with(http_version=CurlHttpVersion.V1_1)
        self.assertEqual(content, "schedule")
        self.assertEqual(session.get.call_count, 2)
        for call in session.get.call_args_list:
            self.assertIs(call.kwargs["verify"], True)

    def test_telus_disables_http2(self):
        from app.epg_platform.Telus import TelusPlatform

        with patch("curl_cffi.requests.Session") as create_session:
            TelusPlatform()._get_session()
        create_session.assert_called_once_with(http_version=CurlHttpVersion.V1_1)
