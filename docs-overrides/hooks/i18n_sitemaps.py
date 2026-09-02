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

"""Expose the combined sitemap at the language roots requested by Material."""

from pathlib import Path
from shutil import copyfile

from mkdocs import plugins


@plugins.event_priority(-110)
def on_post_build(config, **kwargs):
    """Copy sitemaps after i18n has finished all of its nested language builds."""
    i18n = config.plugins["i18n"]
    if i18n.building:
        return

    site_dir = Path(config.site_dir)
    for locale in i18n.build_languages:
        if locale == i18n.default_language:
            continue
        for filename in ("sitemap.xml", "sitemap.xml.gz"):
            source = site_dir / filename
            if source.is_file():
                # i18n emits one combined sitemap at the site root. Material
                # resolves sitemap.xml against each alternate language URL.
                destination = site_dir / locale / filename
                destination.parent.mkdir(parents=True, exist_ok=True)
                copyfile(source, destination)
