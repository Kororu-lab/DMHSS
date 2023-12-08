import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import os

# MongoDB setup
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'stat_comments'
SUBMISSIONS_COLLECTION_NAME = 'stat_submissions'
USER_COLLECTION_NAME = 'stat_submissions'  # Updated collection name for users
OUTPUT_DIR = "./step4/"
BATCH_SIZE = 500  # Number of users to process in each batch

# Scoring weights and adjustments
COMMENT_SCORE_WEIGHT = 1
SUBMISSION_SCORE_WEIGHT = 1
SUBMISSION_COMMENT_WEIGHT = 1
REPEATED_COMMENT_MULTIPLIER = 0
ZERO_SCORE_COMMENT_ADJUSTMENT = 1
ZERO_SCORE_SUBMISSION_ADJUSTMENT = -1

client = MongoClient()
db = client[DB_NAME]

def fetch_target_users(user_group):
    print(f"Connecting to DB... Accessing collection: {USER_COLLECTION_NAME}")
    user_col = db[USER_COLLECTION_NAME]
    try:
        pipeline = [
            {"$match": {"user_grp": user_group}},
            {"$group": {"_id": "$author"}},
            {"$limit": BATCH_SIZE}
        ]
        users = [doc['_id'] for doc in user_col.aggregate(pipeline)]
        # print(f"Users fetched for group {user_group}: {users}")
        return users
    except Exception as e:
        print(f"Error fetching users: {e}")
        return []

def process_comments(user, subreddit_grp, month):
    month_regex = f"^{month}-"  # Ensure month is a string
    comments = db[COMMENTS_COLLECTION_NAME].find({
        "author": user,
        "subreddit_grp": subreddit_grp,
        "created_day": {"$regex": month_regex}
    })
    comment_scores = 0
    unique_thread_ids = set()
    for comment in comments:
        comment_score = comment.get('score', 0)  # Safely getting the score, default to 0 if not found
        if comment_score == 0:
            comment_score = 1 if comment.get('is_submitter', False) == False else -1

        comment_scores += comment_score * COMMENT_SCORE_WEIGHT
        if comment['parent_id'] in unique_thread_ids:
            comment_scores += comment_score * REPEATED_COMMENT_MULTIPLIER
        unique_thread_ids.add(comment['parent_id'])
    return comment_scores

def process_submissions(user, subreddit_grp, month):
    month_regex = f"^{month}-"  # Ensure month is a string
    submissions = db[SUBMISSIONS_COLLECTION_NAME].find({
        "author": user,
        "subreddit_grp": subreddit_grp,
        "created_day": {"$regex": month_regex}
    })
    submission_scores = 0
    for submission in submissions:
        score = submission['score'] if submission['score'] != 0 else ZERO_SCORE_SUBMISSION_ADJUSTMENT
        submission_scores += score * SUBMISSION_SCORE_WEIGHT
        submission_scores += submission['num_comments'] * SUBMISSION_COMMENT_WEIGHT
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

    for user_group in ['SW', 'MH', 'Otr']:
        print(f"Fetching users for group: {user_group}")
        target_users = fetch_target_users(user_group)

        print(f"Found {len(target_users)} users in group {user_group}")

        all_scores = []

        with tqdm(total=len(target_users), desc=f"Processing {user_group} Users") as pbar:
            for user in target_users:
                user_scores = calculate_scores_for_user(user)  # This function will handle all available data
                user_scores['author'] = user
                all_scores.append(user_scores)
                pbar.update(1)

        scores_df = pd.DataFrame(all_scores)
        scores_df.to_csv(os.path.join(OUTPUT_DIR, f"user_interaction_scores_{user_group}.csv"), index=False)

if __name__ == "__main__":
    main()
