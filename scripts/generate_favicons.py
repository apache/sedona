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

"""
Generate all required favicon assets from the source logo PNG.

Usage:
    make genfavicons          # recommended (installs deps via uv first)
    python scripts/generate_favicons.py  # or run directly if Pillow is installed

The source image is docs/image/sedona_logo_symbol.png.
Generated files are written to docs/ (site root in the built site).

Re-run this script (or ``make genfavicons``) whenever
docs/image/sedona_logo_symbol.png is updated, then commit the regenerated
files alongside the source change.
"""

from pathlib import Path

from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_PNG = REPO_ROOT / "docs" / "image" / "sedona_logo_symbol.png"
DOCS_ROOT = REPO_ROOT / "docs"
FAVICONS_DIR = DOCS_ROOT / "favicons"

SIZES = {
    "favicon-16x16.png": (16, 16),
    "favicon-32x32.png": (32, 32),
    "apple-touch-icon.png": (180, 180),
    "mstile-150x150.png": (150, 150),
    "android-chrome-192x192.png": (192, 192),
    "android-chrome-512x512.png": (512, 512),
}


def make_square(source: Image.Image) -> Image.Image:
    """
    Return a square RGBA image with the source centered on a transparent background.
    """
    w, h = source.size
    side = max(w, h)
    square = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    offset = ((side - w) // 2, (side - h) // 2)
    square.paste(source, offset)
    return square


def generate_png(source: Image.Image, size: tuple, dest: Path) -> None:
    sq = make_square(source)
    resized = sq.resize(size, Image.LANCZOS)
    resized.save(dest, format="PNG", optimize=True)
    print(f"  Created {dest.relative_to(REPO_ROOT)}")


def generate_ico(source: Image.Image, dest: Path) -> None:
    # The `sizes` parameter tells Pillow to resize the source to each listed
    # size and pack them all into a single multi-resolution .ico file.
    sq = make_square(source)
    sq.save(dest, format="ICO", sizes=[(16, 16), (32, 32), (48, 48)])
    print(f"  Created {dest.relative_to(REPO_ROOT)}")


def generate_safari_pinned_tab_svg(source: Image.Image, dest: Path) -> None:
    """Generate a monochrome SVG for Safari pinned tabs by tracing the alpha channel.

    Safari mask-icon requires a single-color SVG (all paths filled with one color;
    the browser applies its own tint at render time).  Since the source logo has no
    vector paths we derive the shape from the alpha channel: square the image, scale
    to 100×100, then emit one <rect> per run of opaque pixels per scanline.
    """
    SIZE = 100
    sq = make_square(source)
    small = sq.resize((SIZE, SIZE), Image.LANCZOS)
    alpha = small.getchannel("A")
    pixels = list(alpha.getdata())

    rects = []
    for y in range(SIZE):
        row = pixels[y * SIZE : (y + 1) * SIZE]
        x = 0
        while x < SIZE:
            if row[x] > 127:
                run_start = x
                while x < SIZE and row[x] > 127:
                    x += 1
                rects.append(
                    f'  <rect x="{run_start}" y="{y}" width="{x - run_start}" height="1"/>'
                )
            else:
                x += 1

    svg_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">',
        '  <g fill="#000000">',
        *rects,
        "  </g>",
        "</svg>",
    ]
    dest.write_text("\n".join(svg_lines) + "\n", encoding="utf-8")
    print(f"  Created {dest.relative_to(REPO_ROOT)}")


def main():
    print(f"Loading source: {SOURCE_PNG.relative_to(REPO_ROOT)}")
    source = Image.open(SOURCE_PNG).convert("RGBA")

    FAVICONS_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating PNG favicons...")
    for filename, size in SIZES.items():
        generate_png(source, size, FAVICONS_DIR / filename)

    print("Generating favicon.ico...")
    generate_ico(source, DOCS_ROOT / "favicon.ico")

    print("Generating safari-pinned-tab.svg...")
    generate_safari_pinned_tab_svg(source, FAVICONS_DIR / "safari-pinned-tab.svg")

    print("Done.")


if __name__ == "__main__":
    main()
