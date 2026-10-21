import gzip
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


class APITest(unittest.TestCase):
    def setUp(self):
        # Route tests must not start live provider crawls or the scheduler.
        self.client = TestClient(app)

    def test_health(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "healthy")

    def test_compressed_epg_download_and_range(self):
        content = gzip.compress(b'<?xml version="1.0"?><tv></tv>')
        with tempfile.TemporaryDirectory() as directory:
            xml_path = Path(directory) / "all.xml"
            xml_path.with_suffix(".xml.gz").write_bytes(content)
            with patch(
                "app.main.EPGFileManager.get_latest_epg_file",
                return_value=("20261021", str(xml_path)),
            ):
                response = self.client.get("/all.xml.gz")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, content)
                self.assertEqual(response.headers["content-type"], "application/gzip")
                response = self.client.get("/all.xml.gz", headers={"Range": "bytes=0-9"})
                self.assertEqual(response.status_code, 206)
                self.assertEqual(response.content, content[:10])

    def test_missing_compressed_epg(self):
        with patch("app.main.EPGFileManager.get_latest_epg_file", return_value=(None, None)):
            self.assertEqual(self.client.get("/all.xml.gz").status_code, 404)
