# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Build a small multilingual site and verify Material's sitemap targets.

Run with: python -m unittest discover -s docs-overrides/hooks/tests
"""

import gzip
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from xml.etree import ElementTree

HOOK = Path(__file__).resolve().parents[1] / "i18n_sitemaps.py"


class LocalizedSitemapTest(unittest.TestCase):
    def test_localized_sitemaps(self):
        for version in ("", "latest/", "latest-snapshot/"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                docs = root / "docs"
                docs.mkdir()
                (docs / "index.md").write_text("# Home\n", encoding="utf-8")
                (docs / "index.zh.md").write_text("# Chinese home\n", encoding="utf-8")
                (root / "mkdocs.yml").write_text(
                    f"""site_name: Sitemap test
site_url: https://example.com/{version}
theme:
  name: material
hooks:
  - {HOOK}
plugins:
  - i18n:
      docs_structure: suffix
      languages:
        - locale: en
          default: true
          name: English
          build: true
        - locale: zh
          name: Chinese
          build: true
        - locale: fr
          name: French
          build: false
""",
                    encoding="utf-8",
                )
                result = subprocess.run(
                    [sys.executable, "-m", "mkdocs", "build"],
                    cwd=root,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                site = root / "site"
                sitemap = (site / "sitemap.xml").read_bytes()
                self.assertEqual((site / "zh/sitemap.xml").read_bytes(), sitemap)
                self.assertEqual(
                    gzip.decompress((site / "zh/sitemap.xml.gz").read_bytes()),
                    sitemap,
                )
                locations = {
                    entry.text
                    for entry in ElementTree.fromstring(sitemap).iter(
                        "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                    )
                }
                self.assertIn(f"https://example.com/{version}", locations)
                self.assertIn(f"https://example.com/{version}zh/", locations)
                self.assertFalse((site / "fr/sitemap.xml").exists())
                self.assertFalse((site / "en/sitemap.xml").exists())


if __name__ == "__main__":
    unittest.main()
