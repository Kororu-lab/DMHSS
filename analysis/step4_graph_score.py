from pymongo import MongoClient
from tqdm import tqdm

# MongoDB setup
DB_NAME = 'reddit'
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
USER_COLLECTION_NAME = 'filtered_submissions_standard'  # Updated collection name for users
# BATCH_SIZE = 500  # Number of users to process in each batch

# Scoring weights and adjustments
COMMENT_SCORE_WEIGHT = 3
SUBMISSION_SCORE_WEIGHT = 0.5
SUBMISSION_COMMENT_WEIGHT = 0.5
REPEATED_COMMENT_MULTIPLIER = 9
ZERO_SCORE_COMMENT_ADJUSTMENT = 2
ZERO_SCORE_SUBMISSION_ADJUSTMENT = -0.5

client = MongoClient()
db = client[DB_NAME]

def update_document(user, subreddit_grp, month, score, collection_name):
    query = {"author": user, "subreddit_grp": subreddit_grp, "created_day": {"$regex": f"^{month}-"}}
    update = {"$set": {"graph": score}}
    if collection_name == COMMENTS_COLLECTION_NAME:
        db[COMMENTS_COLLECTION_NAME].update_many(query, update)
    else:
        db[SUBMISSIONS_COLLECTION_NAME].update_many(query, update)

def calculate_and_update_scores(user, subreddit_grp, month, collection, collection_name):
    documents = collection.find({
        "author": user,
        "subreddit_grp": subreddit_grp,
        "created_day": {"$regex": f"^{month}-"}
    })
    total_score = 0
    unique_thread_ids = set()
    
    for doc in documents:
        score = int(doc.get('score', 0))
        # Adjust for zero scores
        if score == 0:
            score = ZERO_SCORE_COMMENT_ADJUSTMENT if collection_name == COMMENTS_COLLECTION_NAME else ZERO_SCORE_SUBMISSION_ADJUSTMENT

        if collection_name == COMMENTS_COLLECTION_NAME:
            total_score += score * COMMENT_SCORE_WEIGHT
            if doc['parent_id'] in unique_thread_ids:
                total_score += score * REPEATED_COMMENT_MULTIPLIER
            unique_thread_ids.add(doc['parent_id'])
        else:
            total_score += score * SUBMISSION_SCORE_WEIGHT + int(doc.get('num_comments', 0)) * SUBMISSION_COMMENT_WEIGHT

    update_document(user, subreddit_grp, month, total_score, collection_name)

def main():
    users = db[USER_COLLECTION_NAME].distinct("author")
    unique_users = list(set(users))

    for user in tqdm(unique_users, desc="Processing Users"):
        unique_months = db[COMMENTS_COLLECTION_NAME].distinct("month", {"author": user}) + db[SUBMISSIONS_COLLECTION_NAME].distinct("month", {"author": user})
        unique_months = list(set(unique_months))

        for month in unique_months:
            for subreddit_grp in ['SW', 'MH', 'Otr']:
                calculate_and_update_scores(user, subreddit_grp, month, db[COMMENTS_COLLECTION_NAME], COMMENTS_COLLECTION_NAME)
                calculate_and_update_scores(user, subreddit_grp, month, db[SUBMISSIONS_COLLECTION_NAME], SUBMISSIONS_COLLECTION_NAME)

if __name__ == "__main__":
    main()
