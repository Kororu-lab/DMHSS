#!/bin/bash

SSD_DIR=$1
comment_file_fullname=$2
submission_file_fullname=$3

# Process the comment file
if [[ $comment_file_fullname == *.zst ]]; then
    BASENAME=$(basename $comment_file_fullname .zst)
    zstd -d -f "$SSD_DIR/compressed/$comment_file_fullname" --long=31 -o "$SSD_DIR/released/$BASENAME.ndjson"
    python3 process_comments.py "$SSD_DIR/released/$BASENAME.ndjson" "$SSD_DIR/processed/$BASENAME.csv"
fi

# Process the submission file
if [[ $submission_file_fullname == *.zst ]]; then
    BASENAME=$(basename $submission_file_fullname .zst)
    zstd -d -f "$SSD_DIR/compressed/$submission_file_fullname" --long=31 -o "$SSD_DIR/released/$BASENAME.ndjson"
    python3 process_submissions.py "$SSD_DIR/released/$BASENAME.ndjson" "$SSD_DIR/processed/$BASENAME.csv"
fi
