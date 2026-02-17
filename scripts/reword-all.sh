#!/bin/sh
# Change every "pick" to "reword" in the rebase todo file (so we can edit each commit message).
sed 's/^pick/reword/' "$1" > "$1.new" && mv "$1.new" "$1"
