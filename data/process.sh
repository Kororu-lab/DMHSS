#!/bin/bash

# Create directories if they don't exist
mkdir -p released processed

# Decompress all files from "compressed" to "released"
for file in compressed/*.zst; do
    BASENAME=$(basename $file .zst)
    zstd -d $file --long=31 -o "released/$BASENAME.ndjson"
done

# Process files in "released" and save results in "processed"
for file in released/*.ndjson; do
    BASENAME=$(basename $file .ndjson)
    if [[ $BASENAME == *_comments || $BASENAME == RC_* ]]; then
        python3 process_comments.py "released/$BASENAME.ndjson" "processed/$BASENAME.csv"
    elif [[ $BASENAME == *_submissions || $BASENAME == RS_* ]]; then
        python3 process_submissions.py "released/$BASENAME.ndjson" "processed/$BASENAME.csv"
    fi
done
