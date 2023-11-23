import pymongo
import csv
from tqdm import tqdm

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
OUTPUT_FILE = "low500_score2_full.csv"  # internal/external/total

# Collection names
COLLECTION_SUBMISSIONS = "filtered_submissions_score2"
COLLECTION_COMMENTS = "filtered_comments_score2"
FULL_REDDIT_COMMENTS = "filtered_comments_score2"
FULL_REDDIT_SUBMISSIONS = "filtered_submissions_score2"

# Weights for submissions and comments
SUBMISSION_WEIGHT = 2
COMMENT_WEIGHT = 1

# Number of top users to extract
TOP_N = 500

# Order can be "ascending" or "descending"
# ORDER = "descending"
ORDER = "ascending"

# Establish connection
client = pymongo.MongoClient()
db = client[DB_NAME]

# MongoDB aggregation pipeline to calculate total scores and counts
def calculate_total_scores_and_counts(collection, weight, score_field):
    pipeline = [
        {"$match": {"author": {"$ne": "[deleted]"}}},
        {"$group": {
            "_id": "$author",
            "total_score": {"$sum": {"$multiply": [f"${score_field}", weight]}},
            "total_weighted_count": {"$sum": weight}
        }}
    ]
    return {doc["_id"]: doc for doc in db[collection].aggregate(pipeline, allowDiskUse=True)}

# Calculate total scores and counts for submissions and comments
submissions_data = calculate_total_scores_and_counts(COLLECTION_SUBMISSIONS, SUBMISSION_WEIGHT, 'sentiment_score_complex')
comments_data = calculate_total_scores_and_counts(COLLECTION_COMMENTS, COMMENT_WEIGHT, 'sentiment_score')

# Calculate integrated scores by combining submissions and comments data
users_scores = {}
for author in submissions_data.keys() | comments_data.keys():
    submissions_count = submissions_data.get(author, {}).get("total_weighted_count", 0) / SUBMISSION_WEIGHT
    comments_count = comments_data.get(author, {}).get("total_weighted_count", 0) / COMMENT_WEIGHT
    total_score = submissions_data.get(author, {}).get("total_score", 0) + comments_data.get(author, {}).get("total_score", 0)
    total_weighted_count = submissions_data.get(author, {}).get("total_weighted_count", 0) + comments_data.get(author, {}).get("total_weighted_count", 0)
    if not (submissions_count == 0): # and comments_count < 3):
        users_scores[author] = total_score / total_weighted_count if total_weighted_count else 0

# Sort users by their integrated scores
sorted_users = sorted(users_scores.items(), key=lambda x: x[1], reverse=(ORDER == "descending"))

# Write to CSV with activities in chunks
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Rank", "User", "Integrated_Score", "Activity_Type", "Subreddit", "Comment/Title", "Selftext", "Date", "Original_Score", "Title_Score", "Text_Score", "Sentiment_Score"])

    for rank, (user, score) in tqdm(enumerate(sorted_users[:TOP_N], 1), desc="Writing to CSV"):
        # Fetch submissions for the user from the FULL collection
        cursor_submissions = db[FULL_REDDIT_SUBMISSIONS].find({"author": user})
        for doc in cursor_submissions:
            writer.writerow([
                rank, user, score, "Submission", doc.get("subreddit", ""), doc.get("title", ""),
                doc.get("selftext", ""), doc.get("created_utc", ""), doc.get("score", 0),
                doc.get("sentiment_score_title", 0), doc.get("sentiment_score_text", 0), doc.get("sentiment_score_complex", 0)
            ])

        # Fetch comments for the user from the FULL collection
        cursor_comments = db[FULL_REDDIT_COMMENTS].find({"author": user})
        for doc in cursor_comments:
            writer.writerow([
                rank, user, score, "Comment", doc.get("subreddit", ""), doc.get("body", ""),
                "", doc.get("created_utc", ""), doc.get("score", 0),
                "", "", doc.get("sentiment_score", 0)
            ])

print(f"Finished generating {OUTPUT_FILE}.")
