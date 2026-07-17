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

# Appends a "Star us on GitHub" call-to-action to the end of every blog post.
# Implemented as an on_page_markdown hook so it applies to all current and
# future posts automatically, with no per-file edits.
#
# The CTA is emitted as Markdown (an admonition plus attr_list buttons) rather
# than raw HTML, so it inherits the Material theme's styling and stays robust
# across theme changes. Requires the `admonition` and `attr_list` extensions,
# which are already enabled in mkdocs.yml.

from mkdocs import plugins

STAR_CTA = """

!!! tip "Star Apache Sedona on GitHub"

    A star takes two seconds and helps others discover the projects.

    [apache/sedona](https://github.com/apache/sedona){ .md-button .md-button--primary target="_blank" rel="noopener" }
    [apache/sedona-db](https://github.com/apache/sedona-db){ .md-button target="_blank" rel="noopener" }
    [apache/sedona-spatialbench](https://github.com/apache/sedona-spatialbench){ .md-button target="_blank" rel="noopener" }
"""


@plugins.event_priority(-50)
def on_page_markdown(markdown, page, config, files, **kwargs):
    src = page.file.src_uri
    if src.startswith("blog/posts/") and src.endswith(".md"):
        return markdown + STAR_CTA
    return markdown
