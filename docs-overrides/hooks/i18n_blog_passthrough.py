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

# Works around the known incompatibility between mkdocs-material's blog plugin
# and mkdocs-static-i18n: the blog plugin generates archive/pagination files
# under a tempfile.mkdtemp() directory, which mkdocs-static-i18n's
# reconfigure_files drops because they live outside docs_dir. Result: the
# rendered /blog/ index has no individual posts.
#
# This hook removes blog files from the Files collection before i18n's on_files
# (priority -100) processes them, then re-adds them after. Net effect: the blog
# is rendered once in the default language and served identically under every
# language version.
#
# Source: https://github.com/ultrabug/mkdocs-static-i18n/issues/283

from mkdocs import plugins
from mkdocs.structure.files import File, Files

BLOG_FILES: list = []


@plugins.event_priority(-95)
def _on_files_disconnect_blog_files(files: Files, config, *_, **__):
    """Remove blog files after the blog plugin's on_files (-50), before i18n's (-100)."""
    global BLOG_FILES
    BLOG_FILES = []
    non_blog_files: list[File] = []
    blog_prefixes: list[str] = []

    for name, instance in config.plugins.items():
        if name.startswith("material/blog"):
            blog_prefixes.append(instance.config.blog_dir)

    blog_prefix_tuple = tuple(p.rstrip("/") + "/" for p in blog_prefixes)
    config.extra.i18n_blog_prefixes = blog_prefix_tuple

    for file in files:
        if file.src_uri.startswith(blog_prefix_tuple):
            BLOG_FILES.append(file)
        else:
            non_blog_files.append(file)

    return Files(non_blog_files)


@plugins.event_priority(-105)
def _on_files_connect_blog_files(files: Files, *_, **__):
    """Restore blog files after i18n's on_files has run."""
    for file in BLOG_FILES:
        files.append(file)
    return files


on_files = plugins.CombinedEvent(
    _on_files_disconnect_blog_files,
    _on_files_connect_blog_files,
)
