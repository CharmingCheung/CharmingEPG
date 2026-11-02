import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from app.epg_platform.FourGTV import FourGTVPlatform, _GaveUp, get_4gtv_epg
from app.epg_platform.base import Channel


class FourGTVBackoffTest(unittest.TestCase):
    def test_repeated_403_aborts_across_channels(self):
        platform = FourGTVPlatform()
        session = Mock()
        session.get.return_value.status_code = 403
        with patch.object(platform, '_get_session', return_value=session), patch(
            'app.epg_platform.FourGTV.time.sleep'
        ), patch('app.epg_platform.FourGTV.time.time', side_effect=lambda: session.get.call_count * 61):
            # Three retries exhaust the first channel; failures on the next
            # channel must stop the whole crawl instead of growing the delay.
            self.assertEqual(platform._fetch_url('https://example.com/first'), '')
            with self.assertRaisesRegex(_GaveUp, '403'):
                platform._fetch_url('https://example.com/second')
        self.assertEqual(session.get.call_count, platform.MAX_403)
        self.assertLess(platform._spacing, 15)

    def test_403_limit_also_applies_inside_penalty_window(self):
        platform = FourGTVPlatform()
        with patch('app.epg_platform.FourGTV.time.sleep'), patch(
            'app.epg_platform.FourGTV.time.time', return_value=100
        ):
            for _ in range(platform.MAX_403 - 1):
                platform._on_403()
            with self.assertRaises(_GaveUp):
                platform._on_403()

    def test_aborted_crawl_does_not_publish_partial_data(self):
        channel = Channel('test', 'test')
        with patch('app.epg_platform.FourGTV.fourgtv_platform') as platform:
            platform.fetch_channels = AsyncMock(return_value=[channel])
            platform.fetch_programs = AsyncMock(side_effect=_GaveUp('403'))
            self.assertEqual(asyncio.run(get_4gtv_epg()), ([], []))
