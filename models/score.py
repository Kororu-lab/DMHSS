import pymongo
import csv
from collections import defaultdict
from tqdm import tqdm

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
OUTPUT_FILE = "low500_suicide2_full.csv" # internal/external/total

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

# Fetch users and their integrated scores
users_scores = defaultdict(lambda: [0.0, 0])  # [total_score, weighted_count]

cursor_submissions = list(db[COLLECTION_SUBMISSIONS].find({"author": {"$ne": "[deleted]"}}))
cursor_comments = list(db[COLLECTION_COMMENTS].find({"author": {"$ne": "[deleted]"}}))

for doc in tqdm(cursor_submissions, desc="Processing Submissions"):
    score = doc.get("sentiment_score_complex", 0)
    users_scores[doc["author"]][0] += score * SUBMISSION_WEIGHT
    users_scores[doc["author"]][1] += SUBMISSION_WEIGHT

for doc in tqdm(cursor_comments, desc="Processing Comments"):
    score = doc.get("sentiment_score", 0)
    users_scores[doc["author"]][0] += score * COMMENT_WEIGHT
    users_scores[doc["author"]][1] += COMMENT_WEIGHT

# Compute the integrated score
users_with_submissions = set([doc["author"] for doc in cursor_submissions])

for user, (total_score, weighted_count) in list(users_scores.items()):
    if user not in users_with_submissions:
        del users_scores[user]
    else:
        users_scores[user] = total_score / weighted_count if weighted_count else 0

# Sort users by their integrated scores
if ORDER == "descending":
    sorted_users = sorted(users_scores.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
else:
    sorted_users = sorted(users_scores.items(), key=lambda x: x[1])[:TOP_N]


# Fetch activities for top users
all_activities = []

for rank, (user, score) in tqdm(enumerate(sorted_users, 1), desc="Gathering Activities", total=TOP_N):
    cursor_submissions = db[FULL_REDDIT_SUBMISSIONS].find({"author": user})
    cursor_comments = db[FULL_REDDIT_COMMENTS].find({"author": user})
    
    for doc in cursor_submissions:
        title = doc.get("title", "")
        text = doc.get("selftext", "")
        subreddit = doc.get("subreddit", "")
        title_score = doc.get("sentiment_score_title", "")
        text_score = doc.get("sentiment_score_text", "")
        combined_score = doc.get("sentiment_score_complex", "")
        orig_score = doc.get("score", "")
        all_activities.append((rank, user, score, "Submission", subreddit, title, text, doc["created_utc"], orig_score, title_score, text_score, combined_score))
    
    for doc in cursor_comments:
        comment = doc.get("body", "")
        subreddit = doc.get("subreddit", "")
        comment_score = doc.get("sentiment_score", "")
        orig_score = doc.get("score", "")
        all_activities.append((rank, user, score, "Comment", subreddit, comment, "", doc["created_utc"], orig_score, "", "", comment_score))

# Write to CSV
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Rank", "User", "Integrated_Score", "Activity_Type", "Subreddit", "Comment/Title", "Selftext", "Date", "Original_Score", "Title_Score", "Text_Score", "Sentiment_Score"])
    for activity in all_activities:
        writer.writerow(activity)

print(f"Finished generating {OUTPUT_FILE}.")
