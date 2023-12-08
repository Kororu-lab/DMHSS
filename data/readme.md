## ./data

This part of repo consists of processing Reddit Archive datasets(more info: https://www.reddit.com/r/DataHoarder/comments/1479c7b/historic_reddit_archives_ongoing_archival_effort/) form *.zst to json, csv, then upload to MongoDB. For uploading used Python, source code available in "/mongo.py"

### auto.sh
"auto.sh" made to automatically process and return column filtered csv, with parallel processing. After setting the directory could easily process and filter out only desired metadata/data from Archive data compressed.