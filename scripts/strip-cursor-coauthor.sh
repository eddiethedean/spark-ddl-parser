#!/bin/sh
# Remove "Co-authored-by: Cursor" lines from the file given as $1 (used as GIT_EDITOR during rebase).
FILE="$1"
if [ -n "$FILE" ] && [ -f "$FILE" ] && grep -q "Co-authored-by: Cursor" "$FILE" 2>/dev/null; then
  grep -v "Co-authored-by: Cursor" "$FILE" > "${FILE}.tmp" && mv "${FILE}.tmp" "$FILE"
fi
true
