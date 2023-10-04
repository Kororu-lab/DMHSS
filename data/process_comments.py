import sys
import csv
import json

def print_progress(count):
    sys.stdout.write(f'\rProcessed {count}k comments')
    sys.stdout.flush()

input_file = sys.argv[1]
output_file = sys.argv[2]

fields = [
    "created_utc", "subreddit", "author", "body", "score",
    "gilded", "is_submitter", "num_reports",
    "stickied", "subreddit_type", "total_awards_received",
    "ups"
]

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fields)
    writer.writeheader()
    count = 0

    for line in infile:
        comment = json.loads(line.strip())
        writer.writerow({field: comment.get(field, None) for field in fields})
        count += 1
        if count % 1000 == 0:
            print_progress(count/1000)
