import asyncio
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.config import Config
from app.epg_platform.Claro import ClaroPlatform


class ClaroPlatformTest(unittest.TestCase):
    def test_fixed_channel_mapping_contains_only_premiere_channels(self):
        platform = ClaroPlatform()

        channels = asyncio.run(platform.fetch_channels())

        self.assertEqual(
            [channel.channel_id for channel in channels],
            ["1365", "1360", "1361", "1362", "1363", "1364", "693"],
        )
        self.assertEqual(
            [channel.name for channel in channels],
            [
                "Premiere Clubes", "Premiere 2", "Premiere 3", "Premiere 4",
                "Premiere 5", "Premiere 6", "Premiere 7",
            ],
        )

    def test_request_params_cover_yesterday_through_seven_days_ahead(self):
        now = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)

        params = ClaroPlatform._request_params(now)

        self.assertEqual(
            params["fq"],
            "dh_inicio:[2026-09-03T00:00:00Z TO 2026-09-11T23:59:59Z]",
        )
        self.assertIn("1_1365", params["q"])
        self.assertIn("1_693", params["q"])
        self.assertNotIn("1_1014", params["q"])

    def test_parse_programs_filters_invalid_and_unknown_records(self):
        payload = {
            "response": {
                "docs": [
                    {
                        "id_canal": "693",
                        "titulo": "Com o PREMIERE dá jogo!",
                        "dh_inicio": "2026-09-04T00:00Z",
                        "dh_fim": "2026-09-04T02:00Z",
                    },
                    {
                        "id_canal": "1014",
                        "titulo": "Mosaico",
                        "dh_inicio": "2026-09-04T00:00Z",
                        "dh_fim": "2026-09-04T02:00Z",
                    },
                    {
                        "id_canal": "1360",
                        "titulo": "Invalid range",
                        "dh_inicio": "2026-09-04T02:00Z",
                        "dh_fim": "2026-09-04T01:00Z",
                    },
                ]
            }
        }

        programs = ClaroPlatform._parse_programs(payload, {"693", "1360"})

        self.assertEqual(len(programs), 1)
        self.assertEqual(programs[0].channel_id, "693")
        self.assertEqual(programs[0].title, "Com o PREMIERE dá jogo!")
        self.assertEqual(programs[0].start_time.tzinfo, timezone.utc)

    def test_claro_is_disabled_by_default_and_can_be_enabled(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("EPG_ENABLE_CLARO", None)
            self.assertFalse(Config.platform_enabled("claro"))
            enabled = {item["platform"] for item in Config.get_enabled_platforms()}
            self.assertNotIn("claro", enabled)

        with patch.dict(os.environ, {"EPG_ENABLE_CLARO": "true"}):
            enabled = {item["platform"] for item in Config.get_enabled_platforms()}
            self.assertIn("claro", enabled)


if __name__ == "__main__":
    unittest.main()
