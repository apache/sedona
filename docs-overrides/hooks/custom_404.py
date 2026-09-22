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

"""Keep the default-language 404 page and point Apache at it.

mkdocs-static-i18n builds English then Chinese. The Chinese nested build
overwrites site/404.html, so /latest/404.html would otherwise ship in zh.
This hook restores the English page after every language has finished, and
writes a per-version .htaccess so missing URLs under that version use the
branded 404 instead of Apache's default Not Found page.
"""

from pathlib import Path

from mkdocs import plugins

_DEFAULT_404 = None

_HTACCESS = """\
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

ErrorDocument 404 /latest/404.html
"""


@plugins.event_priority(-110)
def on_post_build(config, **kwargs):
    global _DEFAULT_404
    site_dir = Path(config.site_dir)
    html = site_dir / "404.html"
    i18n = config.plugins.get("i18n")

    if i18n is not None and i18n.building:
        if config.theme.language == i18n.default_language and html.is_file():
            _DEFAULT_404 = html.read_bytes()
        return

    if _DEFAULT_404 is not None:
        html.write_bytes(_DEFAULT_404)

    (site_dir / ".htaccess").write_text(_HTACCESS, encoding="utf-8")
