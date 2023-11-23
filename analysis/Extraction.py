import pymongo
import csv
from tqdm import tqdm

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
INPUT_FILE = "extraction_test.csv"  # CSV file containing 'rank' and 'author'
OUTPUT_FILE = "extracted_test_return.csv"

# Collection names
FULL_REDDIT_COMMENTS = "filtered_comments"
FULL_REDDIT_SUBMISSIONS = "filtered_submissions"

# Establish connection
client = pymongo.MongoClient()
db = client[DB_NAME]

# Read the input CSV file to get the list of users
with open(INPUT_FILE, 'r') as file:
    reader = csv.DictReader(file)
    users = [(int(row['rank']), row['author']) for row in reader]

# Write to CSV with activities
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Rank", "User", "Activity_Type", "Subreddit", "Comment/Title", "Selftext", "Date", "Original_Score", "Title_Score", "Text_Score", "Sentiment_Score"])

    for rank, user in tqdm(users, desc="Processing Users"):
        # Fetch submissions for the user
        cursor_submissions = db[FULL_REDDIT_SUBMISSIONS].find({"author": user})
        for doc in cursor_submissions:
            writer.writerow([
                rank, user, "Submission", doc.get("subreddit", ""), doc.get("title", ""),
                doc.get("selftext", ""), doc.get("created_utc", ""), doc.get("score", 0),
                doc.get("sentiment_score_title", 0), doc.get("sentiment_score_text", 0), doc.get("sentiment_score_complex", 0)
            ])

        # Fetch comments for the user
        cursor_comments = db[FULL_REDDIT_COMMENTS].find({"author": user})
        for doc in cursor_comments:
            writer.writerow([
                rank, user, "Comment", doc.get("subreddit", ""), doc.get("body", ""),
                "", doc.get("created_utc", ""), doc.get("score", 0),
                "", "", doc.get("sentiment_score", 0)
            ])

print(f"Finished generating {OUTPUT_FILE}.")
