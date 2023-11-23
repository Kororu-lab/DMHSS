#!/bin/bash

# Define directories
HDD_DIR_COMMENTS="/mnt/d20_main/N99_Project_Data/Reddit/reddit/comments"
HDD_DIR_SUBMISSIONS="/mnt/d20_main/N99_Project_Data/Reddit/reddit/submissions"
SSD_DIR="/home/kororu/DMHSS/data"
HDD_DIR_PROCESSED="/mnt/t7/REDDIT/workload"

# Fetch list of files from the directories
comments_files=($(ls "$HDD_DIR_COMMENTS"/*.zst))
submissions_files=($(ls "$HDD_DIR_SUBMISSIONS"/*.zst))

process_files() {
    local comment_file="$1"
    local submission_file="$2"
    
    # Copy files to SSD for processing
    echo "Copying $comment_file to SSD..."
    cp "$comment_file" "$SSD_DIR/compressed/"
    
    echo "Copying $submission_file to SSD..."
    cp "$submission_file" "$SSD_DIR/compressed/"
    
    # Print the files in the compressed directory for debugging
    echo "Files in the compressed directory:"
    ls "$SSD_DIR/compressed/"
    
    # Execute the processing script
    echo "Processing files..."
    bash "$SSD_DIR/process.sh"
    
    # Move processed files to the HDD processed directory
    echo "Moving processed files to HDD..."
    sudo mv "$SSD_DIR/processed/"*.csv "$HDD_DIR_PROCESSED/"
    
    # Clean up the SSD compressed and released directories
    echo "Cleaning up SSD directory..."
    rm -fv "$SSD_DIR/compressed/"*
    # echo "Cleaning up SSD released directory..."
    rm -fv "$SSD_DIR/released/"*
    
    echo "Processed files: $(basename "$comment_file") and $(basename "$submission_file")"
}

i=0
while [ $i -lt ${#comments_files[@]} ]; do
    # Run 3 sets of file pairs in parallel
    process_files "${comments_files[$i]}" "${submissions_files[$i]}" &
    process_files "${comments_files[$i+1]}" "${submissions_files[$i+1]}" &
    process_files "${comments_files[$i+2]}" "${submissions_files[$i+2]}" &
    
    # Wait for all 3 sets to finish before moving to the next set
    wait
    
    # Increment by 3 to process next set of files
    i=$((i+3))
done

echo "Batch processing completed!"
