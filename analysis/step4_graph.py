import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import os

# MongoDB setup
DB_NAME = 'reddit'
USER_COLLECTION_NAME = 'filtered_submissions_standard'  # Collection to fetch target users
COMMENTS_COLLECTION_NAME = 'filtered_comments_standard'
SUBMISSIONS_COLLECTION_NAME = 'filtered_submissions_standard'
OUTPUT_DIR = "./step4/"
BATCH_SIZE = 500  # Batch size for processing

# Scoring Weights (Hyperparameters)
BREADTH_WEIGHT = 1
DEPTH_WEIGHT = 1
SUBREDDIT_WEIGHTS = {'SW': 1, 'MH': 1, 'Otr': 1}

client = MongoClient()
db = client[DB_NAME]
user_col = db[USER_COLLECTION_NAME]

def fetch_target_users():
    """ Fetch a list of target users from the specified collection. """
    pipeline = [
        {"$group": {"_id": "$author"}},
        {"$limit": BATCH_SIZE}
    ]
    return [doc['_id'] for doc in user_col.aggregate(pipeline)]

def calculate_interaction_metrics(target_users):
    subreddit_weights = {
        'SW': SUBREDDIT_WEIGHTS['SW'],
        'MH': SUBREDDIT_WEIGHTS['MH'],
        'Otr': SUBREDDIT_WEIGHTS['Otr']
    }

    pipeline = [
        {"$match": {"author": {"$in": target_users}}},
        {"$unionWith": {
            "coll": SUBMISSIONS_COLLECTION_NAME,
            "pipeline": [{"$project": {"author": 1, "subreddit_grp": 1, "created_day": 1, "parent_id": 1}}]
        }},
        {"$addFields": {"month": {"$substr": ["$created_day", 0, 7]}}},
        {"$group": {
            "_id": {"author": "$author", "subreddit_grp": "$subreddit_grp", "month": "$month"},
            "unique_reply_authors": {"$addToSet": "$parent_author"},
            "total_replies": {"$sum": 1}
        }},
        {"$project": {
            "author": "$_id.author",
            "subreddit_grp": "$_id.subreddit_grp",
            "month": "$_id.month",
            "interaction_breadth": {"$size": "$unique_reply_authors"},
            "interaction_depth": {
                "$cond": {
                    "if": {"$gt": [{"$size": "$unique_reply_authors"}, 0]},
                    "then": {"$divide": ["$total_replies", {"$size": "$unique_reply_authors"}]},
                    "else": 0
                }
            },
            "subreddit_weight": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$subreddit_grp", "SW"]}, "then": subreddit_weights['SW']},
                        {"case": {"$eq": ["$subreddit_grp", "MH"]}, "then": subreddit_weights['MH']},
                        {"case": {"$eq": ["$subreddit_grp", "Otr"]}, "then": subreddit_weights['Otr']}
                    ],
                    "default": 1
                }
            }
        }},
        {"$group": {
            "_id": {"author": "$author", "month": "$month"},
            "metrics": {
                "$push": {
                    "subreddit_grp": "$subreddit_grp",
                    "breadth": "$interaction_breadth",
                    "depth": "$interaction_depth",
                    "subreddit_weight": "$subreddit_weight"
                }
            }
        }},
        {"$project": {
            "author": "$_id.author",
            "month": "$_id.month",
            "scores": {
                "$map": {
                    "input": "$metrics",
                    "as": "metric",
                    "in": {
                        "subreddit_grp": "$$metric.subreddit_grp",
                        "score": {
                            "$add": [
                                {"$multiply": ["$$metric.breadth", BREADTH_WEIGHT]},
                                {"$multiply": ["$$metric.depth", DEPTH_WEIGHT]},
                                {"$multiply": ["$$metric.subreddit_weight", {"$add": [BREADTH_WEIGHT, DEPTH_WEIGHT]}]}
                            ]
                        }
                    }
                }
            }
        }}
    ]

    return pd.DataFrame(list(db[COMMENTS_COLLECTION_NAME].aggregate(pipeline, batchSize=BATCH_SIZE)))

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    target_users = fetch_target_users()
    print(f"Total target users: {len(target_users)}")

    interaction_metrics_df = pd.DataFrame()

    with tqdm(total=len(target_users), desc="Processing Users") as pbar:
        for i in range(0, len(target_users), BATCH_SIZE):
            batch_users = target_users[i:i + BATCH_SIZE]
            batch_metrics_df = calculate_interaction_metrics(batch_users)
            interaction_metrics_df = pd.concat([interaction_metrics_df, batch_metrics_df])
            pbar.update(len(batch_users))

    output_file = os.path.join(OUTPUT_DIR, "interaction_metrics.csv")
    interaction_metrics_df.to_csv(output_file, index=False)
    print(f"Interaction metrics exported to {output_file}")

if __name__ == "__main__":
    main()
