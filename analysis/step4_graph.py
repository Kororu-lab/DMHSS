import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import os
import seaborn as sns
import matplotlib.pyplot as plt

# MongoDB setup
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
USER_COLLECTION_NAME = 'filtered_submissions_standard'  # Collection containing target users
OUTPUT_DIR = "./step4/"
BATCH_SIZE = 500  # Number of users to process in each batch

# Scoring weights
COMMENT_SCORE_WEIGHT = 1
SUBMISSION_SCORE_WEIGHT = 2
SUBMISSION_COMMENT_WEIGHT = 2
REPEATED_COMMENT_MULTIPLIER = 4  # Multiplier for repeated comments on the same thread

client = MongoClient()
db = client[DB_NAME]
user_col = db[USER_COLLECTION_NAME]

def fetch_target_users():
    pipeline = [
        {"$group": {"_id": "$author"}},
        {"$limit": BATCH_SIZE}
    ]
    return [doc['_id'] for doc in user_col.aggregate(pipeline)]

def process_comments(user, subreddit_grp, month):
    comments = db[COMMENTS_COLLECTION_NAME].find({
        "author": user,
        "subreddit_grp": subreddit_grp,
        "created_day": {"$regex": f"^{month}-"}
    })
    comment_scores = 0
    unique_thread_ids = set()
    for comment in comments:
        comment_scores += comment['score'] * COMMENT_SCORE_WEIGHT
        if comment['parent_id'] in unique_thread_ids:
            comment_scores += comment['score'] * REPEATED_COMMENT_MULTIPLIER
        unique_thread_ids.add(comment['parent_id'])
    return comment_scores

def process_submissions(user, subreddit_grp, month):
    submissions = db[SUBMISSIONS_COLLECTION_NAME].find({
        "author": user,
        "subreddit_grp": subreddit_grp,
        "created_day": {"$regex": f"^{month}-"}
    })
    submission_scores = 0
    for submission in submissions:
        submission_scores += (submission['score'] * SUBMISSION_SCORE_WEIGHT +
                              submission['num_comments'] * SUBMISSION_COMMENT_WEIGHT)
    return submission_scores

def calculate_scores_for_user(user):
    scores = {}

    # Fetch all unique months for the given user from both comments and submissions
    unique_months_comments = db[COMMENTS_COLLECTION_NAME].distinct("month", {"author": user})
    unique_months_submissions = db[SUBMISSIONS_COLLECTION_NAME].distinct("month", {"author": user})
    unique_months = list(set(unique_months_comments + unique_months_submissions))

    for month in unique_months:
        for subreddit_grp in ['SW', 'MH', 'Otr']:
            scores[f"{subreddit_grp}-com-{month}"] = process_comments(user, subreddit_grp, month)
            scores[f"{subreddit_grp}-sub-{month}"] = process_submissions(user, subreddit_grp, month)

    return scores


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    target_users = fetch_target_users()

    all_scores = []

    with tqdm(total=len(target_users), desc="Processing Users") as pbar:
        for user in target_users:
            user_scores = calculate_scores_for_user(user)  # This function will handle all available data
            user_scores['author'] = user
            all_scores.append(user_scores)
            pbar.update(1)

    scores_df = pd.DataFrame(all_scores)
    scores_df.to_csv(os.path.join(OUTPUT_DIR, "user_interaction_scores.csv"), index=False)

if __name__ == "__main__":
    main()
