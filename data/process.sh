#!/bin/bash

# Process only the passed files
comment_file=$1
submission_file=$2

# Decompress and process the comment file
if [[ -n $comment_file ]]; then
    BASENAME=$(basename $comment_file .zst)
    zstd -d -f "$SSD_DIR/compressed/$comment_file" --long=31 -o "$SSD_DIR/released/$BASENAME.ndjson"
    python3 process_comments.py "$SSD_DIR/released/$BASENAME.ndjson" "$SSD_DIR/processed/$BASENAME.csv"
fi

# Decompress and process the submission file
if [[ -n $submission_file ]]; then
    BASENAME=$(basename $submission_file .zst)
    zstd -d -f "$SSD_DIR/compressed/$submission_file" --long=31 -o "$SSD_DIR/released/$BASENAME.ndjson"
    python3 process_submissions.py "$SSD_DIR/released/$BASENAME.ndjson" "$SSD_DIR/processed/$BASENAME.csv"
fi
