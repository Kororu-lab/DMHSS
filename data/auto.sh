#!/bin/bash

# Define directories
HDD_DIR_COMMENTS="/mnt/LaCie_2/reddit/comments"
HDD_DIR_SUBMISSIONS="/mnt/LaCie_2/reddit/submissions"
SSD_DIR="/home/kororu/DMHSS/data"
HDD_DIR_PROCESSED="/home/kororu/DMHSS/donefile"

# Fetch list of files from the directories
comments_files=($(ls "$HDD_DIR_COMMENTS"/*.zst))
submissions_files=($(ls "$HDD_DIR_SUBMISSIONS"/*.zst))

process_files() {
    local comment_file="$1"
    local submission_file="$2"
    local comment_basename=$(basename "$comment_file")
    local submission_basename=$(basename "$submission_file")

    echo "Processing $comment_basename and $submission_basename..."

    # Copy files to SSD for processing
    echo "Copying $comment_file to $SSD_DIR/compressed/"
    cp "$comment_file" "$SSD_DIR/compressed/"
    
    echo "Copying $submission_file to $SSD_DIR/compressed/"
    cp "$submission_file" "$SSD_DIR/compressed/"
    
    # Execute the processing script for specific files with full filenames
    echo "Running process.sh for $comment_basename and $submission_basename"   
    bash "$SSD_DIR/process.sh" "$SSD_DIR" "$comment_basename" "$submission_basename"

    # Corrected file basename to exclude '.zst' extension for processed file check
    local processed_comment_basename=$(basename "$comment_file" .zst)
    local processed_submission_basename=$(basename "$submission_file" .zst)

    # Check if processed files exist
    echo "Checking for processed files..."
    if [[ -f "$SSD_DIR/processed/${processed_comment_basename}.csv" ]]; then
        echo "Moving ${processed_comment_basename}.csv to $HDD_DIR_PROCESSED"
        mv "$SSD_DIR/processed/${processed_comment_basename}.csv" "$HDD_DIR_PROCESSED/"
    else
        echo "Processed file ${processed_comment_basename}.csv not found."
    fi

    if [[ -f "$SSD_DIR/processed/${processed_submission_basename}.csv" ]]; then
        echo "Moving ${processed_submission_basename}.csv to $HDD_DIR_PROCESSED"
        mv "$SSD_DIR/processed/${processed_submission_basename}.csv" "$HDD_DIR_PROCESSED/"
    else
        echo "Processed file ${processed_submission_basename}.csv not found."
    fi
    
    # Cleanup section
    echo "Cleaning up files for $comment_basename and $submission_basename..."
    local compressed_comment_path="$SSD_DIR/compressed/$comment_basename"
    local compressed_submission_path="$SSD_DIR/compressed/$submission_basename"
    local released_comment_path="$SSD_DIR/released/$(basename "$comment_basename" .zst).ndjson"
    local released_submission_path="$SSD_DIR/released/$(basename "$submission_basename" .zst).ndjson"

    if [[ -f "$compressed_comment_path" ]]; then
        rm -fv "$compressed_comment_path"
    else
        echo "File not found for deletion: $compressed_comment_path"
    fi

    if [[ -f "$compressed_submission_path" ]]; then
        rm -fv "$compressed_submission_path"
    else
        echo "File not found for deletion: $compressed_submission_path"
    fi

    if [[ -f "$released_comment_path" ]]; then
        rm -fv "$released_comment_path"
    else
        echo "File not found for deletion: $released_comment_path"
    fi

    if [[ -f "$released_submission_path" ]]; then
        rm -fv "$released_submission_path"
    else
        echo "File not found for deletion: $released_submission_path"
    fi

    echo "Finished processing $comment_basename and $submission_basename"
}


i=0
while [ $i -lt ${#comments_files[@]} ]; do
    # Run 3 sets of file pairs in parallel
    process_files "${comments_files[$i]}" "${submissions_files[$i]}" &
    #process_files "${comments_files[$i+1]}" "${submissions_files[$i+1]}" &
    #process_files "${comments_files[$i+2]}" "${submissions_files[$i+2]}" &
    
    # Wait for all 3 sets to finish before moving to the next set
    wait
    
    # Increment by 3 to process next set of files
    i=$((i+1)) #3
done

echo "Batch processing completed!"
