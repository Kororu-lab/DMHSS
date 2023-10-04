import sys
import csv
import json

def print_progress(count):
    sys.stdout.write(f'\rProcessed {count}k submissions')
    sys.stdout.flush()

input_file = sys.argv[1]
output_file = sys.argv[2]

fields = [
    "created_utc", "subreddit_name_prefixed", "author", "title", "score",
    "num_comments", "subreddit_type", "num_reports", "controversiality"
]

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fields)
    writer.writeheader()
    count = 0

    for line in infile:
        submission = json.loads(line.strip())
        writer.writerow({field: submission.get(field, None) for field in fields})
        count += 1
        if count % 1000 == 0:
            print_progress(count/1000)
