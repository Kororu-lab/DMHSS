import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import os

# MongoDB setup
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
USER_COLLECTION_NAME = 'filtered_submissions_standard'  # Updated collection name for users
OUTPUT_DIR = "./step4/"
BATCH_SIZE = 500  # Number of users to process in each batch

# Scoring weights and adjustments
COMMENT_SCORE_WEIGHT = 3
SUBMISSION_SCORE_WEIGHT = 0.5
SUBMISSION_COMMENT_WEIGHT = 0.5
REPEATED_COMMENT_MULTIPLIER = 9
ZERO_SCORE_COMMENT_ADJUSTMENT = 2
ZERO_SCORE_SUBMISSION_ADJUSTMENT = -0.5

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
        # Convert comment score to integer
        comment_score = int(comment.get('score', 0))

        # Adjust score based on the scoring logic
        if comment_score == 0:
            if not comment.get('is_submitter', False):  # If the user is not the submitter
                comment_score = 1
            else:  # If the user is the submitter
                comment_score = -1

        # Calculate the weighted score
        comment_scores += comment_score * COMMENT_SCORE_WEIGHT

        # Check for repeated comments
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
        # Convert submission score to integer
        score = int(submission.get('score', 0))

        # Adjust score based on the scoring logic
        if score == 0:
            if submission.get('num_comments', 0) == 0:  # If there are no comments
                score = -1
            else:  # If there are comments
                score = 1

        # Convert num_comments to integer and calculate the weighted score
        num_comments = int(submission.get('num_comments', 0))
        weighted_score = score * SUBMISSION_SCORE_WEIGHT + num_comments * SUBMISSION_COMMENT_WEIGHT
        submission_scores += weighted_score

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
