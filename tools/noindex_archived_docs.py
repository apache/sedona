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

"""Mark archived documentation versions as noindex for search engines.

The documentation site (https://sedona.apache.org) is a mike-managed, versioned
MkDocs site. Only the current stable docs, served under ``/latest/``, should
appear in search engine indexes; older versions and the development snapshot are
stale or duplicate content that otherwise competes with the current docs in
search results.

robots.txt ``Disallow`` cannot be used for this: a blocked page can no longer be
crawled, so search engines never observe a removal signal and may keep stale
URLs in the index (Google explicitly advises against robots.txt for removal).
Instead we add a crawlable

    <meta name="robots" content="noindex, follow">

to every page of the archived versions and leave them crawlable, which lets
search engines drop them from their index while still following links.

Archived versions are frozen once released and are never rebuilt, so this runs
as a post-deploy step over the published ``website`` branch rather than at build
time. It is idempotent: pages that already carry a robots meta tag are left
untouched, so re-running it only changes versions that were newly archived.

The current stable version is deliberately skipped: ``latest`` is a symlink to
that version's directory, so tagging it would de-index the live docs. Its
duplicate versioned URL (for example ``/1.9.0/``) is already consolidated onto
``/latest/`` by the canonical tag that mike emits.

Usage:
    python3 tools/noindex_archived_docs.py <website-branch-checkout>
"""

import os
import re
import sys

ROBOTS_META = '<meta name="robots" content="noindex, follow">'

# Opening <head> tag, allowing attributes (e.g. <head class="...">).
HEAD_RE = re.compile(r"(<head\b[^>]*>)", re.IGNORECASE)
# A complete existing robots meta tag (name= and content= in any order).
ROBOTS_META_RE = re.compile(
    r"""<meta\b[^>]*\bname=["']robots["'][^>]*>""", re.IGNORECASE
)
# The content="..." value of a meta tag.
ROBOTS_CONTENT_RE = re.compile(r"""content=["']([^"']*)["']""", re.IGNORECASE)
# Archived version directories all begin with a digit (e.g. 1.4.1).
VERSION_DIR_RE = re.compile(r"^\d")

ROBOTS_TXT = """\
# robots.txt for https://sedona.apache.org/
#
# Only the current stable documentation, served under /latest/, is indexed.
# Archived versions and the development snapshot instead carry a crawlable
# <meta name="robots" content="noindex"> tag (added by
# tools/noindex_archived_docs.py) so search engines can visit them and drop the
# stale pages. They are intentionally NOT disallowed here, because a disallowed
# page cannot be crawled and its noindex would never be seen.
User-agent: *
Disallow:

Sitemap: https://sedona.apache.org/latest/sitemap.xml
"""


def is_archived_dir(name, current):
    """Return True if ``name`` is a version directory that should be noindexed."""
    if name == "latest" or name == current:
        return False
    return name == "latest-snapshot" or bool(VERSION_DIR_RE.match(name))


def _directives(tag):
    """Return the directive list from a robots meta tag's content attribute."""
    match = ROBOTS_CONTENT_RE.search(tag)
    if not match:
        return []
    return [d.strip() for d in match.group(1).split(",") if d.strip()]


def _add_noindex(tag):
    """Rewrite a robots meta tag so it forbids indexing, keeping other rules.

    Returns the tag unchanged if it already contains ``noindex``. Otherwise
    drops a conflicting ``index`` directive, prepends ``noindex``, and preserves
    the rest (e.g. ``noarchive``). A robots tag with no content attribute is
    replaced outright.
    """
    directives = _directives(tag)
    if any(d.lower() == "noindex" for d in directives):
        return tag
    if not ROBOTS_CONTENT_RE.search(tag):
        return ROBOTS_META
    kept = ["noindex"] + [d for d in directives if d.lower() != "index"]
    return ROBOTS_CONTENT_RE.sub(f'content="{", ".join(kept)}"', tag, count=1)


def inject(path):
    """Add a noindex directive to one HTML file. Return True if it changed."""
    with open(path, encoding="utf-8", errors="surrogatepass") as handle:
        html = handle.read()

    existing = ROBOTS_META_RE.search(html)
    if existing:
        replacement = _add_noindex(existing.group(0))
        if replacement == existing.group(0):
            return False  # already noindex; leave it untouched
        new_html = html[: existing.start()] + replacement + html[existing.end() :]
    else:
        new_html, replaced = HEAD_RE.subn(r"\1\n    " + ROBOTS_META, html, count=1)
        if replaced == 0:
            # No <head> (e.g. a raw HTML fragment such as an embedded form page).
            # Prepend the tag: an HTML parser hoists a leading <meta> into <head>.
            new_html = ROBOTS_META + "\n" + html

    with open(path, "w", encoding="utf-8", errors="surrogatepass") as handle:
        handle.write(new_html)
    return True


def write_robots(root):
    """Write a sitemap-only robots.txt, but only if it is missing or stale."""
    path = os.path.join(root, "robots.txt")
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as handle:
            if handle.read() == ROBOTS_TXT:
                return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(ROBOTS_TXT)
    print("wrote robots.txt")


def main(root):
    # The current stable version is whatever `latest` points at; it must be
    # identified so it is never tagged (tagging it would de-index the live docs,
    # since /latest/ is a symlink to it). If `latest` is missing, is not a
    # symlink, or points nowhere, fail loudly rather than tag every version.
    latest = os.path.join(root, "latest")
    if not os.path.islink(latest):
        print(
            f"error: '{latest}' is missing or is not a symlink; "
            "cannot identify the current stable version",
            file=sys.stderr,
        )
        return 1
    current = os.path.basename(os.path.realpath(latest))
    if not os.path.isdir(os.path.join(root, current)):
        print(
            f"error: 'latest' resolves to '{current}', "
            f"which is not a directory under '{root}'",
            file=sys.stderr,
        )
        return 1

    targets = sorted(
        name
        for name in os.listdir(root)
        if os.path.isdir(os.path.join(root, name))
        and not os.path.islink(os.path.join(root, name))
        and is_archived_dir(name, current)
    )
    print(f"current stable version: {current}")
    print(f"archived versions to noindex: {', '.join(targets) or '(none)'}")

    total = 0
    for name in targets:
        count = 0
        for dirpath, _dirs, files in os.walk(os.path.join(root, name)):
            for filename in files:
                if filename.endswith(".html") and inject(
                    os.path.join(dirpath, filename)
                ):
                    count += 1
        print(f"  {name}: tagged {count} page(s)")
        total += count

    write_robots(root)
    print(f"total pages tagged: {total}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
