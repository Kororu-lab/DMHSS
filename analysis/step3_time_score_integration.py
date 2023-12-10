import pymongo
import pandas as pd
from tqdm import tqdm

# MongoDB Connection Parameters
DB_NAME = "reddit"
COMMENTS_COLLECTION = "filtered_comments_standard"
SUBMISSIONS_COLLECTION = "filtered_submissions_standard"
AGGREGATED_SCORE_FIELD = "time_based_score"
NORMALIZE_SCORES = True  # Set to True to normalize scores before aggregation
WEIGHTS = {"comm_score": 0.5, "time_based": 0.5}  # Weights for each score type

client = pymongo.MongoClient()
db = client[DB_NAME]

def normalize_scores(scores, score_field):
    min_score = scores[score_field].min()
    max_score = scores[score_field].max()
    scores[score_field] = (scores[score_field] - min_score) / (max_score - min_score)
    return scores

def calculate_and_update_aggregated_scores(db, normalize_scores_flag):
    pipeline = [
        {
            "$group": {
                "_id": "$author",
                "avg_comm_score": {"$avg": "$comm_score"},
                "avg_time_based": {"$avg": "$time_based"}
            }
        }
    ]

    for collection_name in [COMMENTS_COLLECTION, SUBMISSIONS_COLLECTION]:
        results = list(db[collection_name].aggregate(pipeline))
        for result in tqdm(results, desc=f"Updating {collection_name}"):
            scores_df = pd.DataFrame([result])

            if normalize_scores_flag:
                scores_df = normalize_scores(scores_df, 'avg_comm_score')
                scores_df = normalize_scores(scores_df, 'avg_time_based')

            aggregated_score = (scores_df['avg_comm_score'].iloc[0] * WEIGHTS['comm_score'] +
                                scores_df['avg_time_based'].iloc[0] * WEIGHTS['time_based'])

            # Update each author's document in the respective collection
            query = {"author": result["_id"]}
            update = {"$set": {AGGREGATED_SCORE_FIELD: aggregated_score}}
            db[collection_name].update_many(query, update)

def main():
    calculate_and_update_aggregated_scores(db, NORMALIZE_SCORES)

if __name__ == "__main__":
    main()
