import sys
import csv
import json

def print_progress(count):
    sys.stdout.write(f'\rProcessed {count}k submissions')
    sys.stdout.flush()

input_file = sys.argv[1]
output_file = sys.argv[2]

fields = [
    "id", "created_utc", "subreddit", "author", "title", "selftext", "score", "num_comments"
]

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fields, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    count = 0

    for line in infile:
        submission = json.loads(line.strip())
        writer.writerow({field: submission.get(field, None) for field in fields})
        count += 1
        if count % 1000 == 0:
            print_progress(count/1000)