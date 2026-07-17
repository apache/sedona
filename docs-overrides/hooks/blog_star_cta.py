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

from mkdocs import plugins

STAR_CTA = """

<div class="sedona-star-cta" style="margin-top:3rem;padding:1.5rem 1.5rem 1.75rem;border:1px solid var(--md-default-fg-color--lightest);border-radius:.6rem;background:var(--md-default-fg-color--lightest)">
  <p style="font-weight:700;font-size:1.1rem;margin:0 0 .25rem">&#11088; Enjoyed this? Star Apache Sedona on GitHub</p>
  <p style="margin:0 0 1rem;color:var(--md-default-fg-color--light)">A star takes two seconds and helps others discover the projects.</p>
  <a class="md-button md-button--primary" href="https://github.com/apache/sedona" target="_blank" rel="noopener">&#11088; apache/sedona</a>
  <a class="md-button" href="https://github.com/apache/sedona-db" target="_blank" rel="noopener">&#11088; apache/sedona-db</a>
  <a class="md-button" href="https://github.com/apache/sedona-spatialbench" target="_blank" rel="noopener">&#11088; apache/sedona-spatialbench</a>
</div>
"""


@plugins.event_priority(-50)
def on_page_markdown(markdown, page, config, files, **kwargs):
    src = page.file.src_uri
    if src.startswith("blog/posts/") and src.endswith(".md"):
        return markdown + STAR_CTA
    return markdown
