#!/bin/bash
# Renders the SedonaDB release post notebook and updates the blog post

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="/Users/dewey/gh/sedona-db/.venv"
NOTEBOOK_PATH="/Users/dewey/gh/sedona-db/docs/release-post.ipynb"
BLOG_POST="$SCRIPT_DIR/intro-sedonadb-0-4.md"

source "$VENV_PATH/bin/activate"

# Render notebook to markdown
jupyter nbconvert --to markdown --stdout "$NOTEBOOK_PATH" > /tmp/rendered-notebook.md

# Extract frontmatter (everything up to and including the second ---)
awk '/^---$/{count++} count==2{print; exit} {print}' "$BLOG_POST" > /tmp/frontmatter.md

# Combine frontmatter with rendered notebook
cat /tmp/frontmatter.md /tmp/rendered-notebook.md > "$BLOG_POST"

echo "Updated $BLOG_POST"
