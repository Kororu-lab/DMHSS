import pymongo
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

# Hyperparameters
DB_NAME = "reddit"  # Replace with your database name
SUBMISSION_COLLECTION_NAME = "filtered_submissions"  # Replace with your submissions collection name
COMMENT_COLLECTION_NAME = "filtered_comments"  # Replace with your comments collection name
TIME_WINDOW = "1D"  # Example: '1D' for daily, '1W' for weekly
BATCH_SIZE = 10000  # Adjust based on memory capacity

def connect_to_mongodb(db_name, collection_name):
    client = pymongo.MongoClient()  # Add connection string if needed
    db = client[db_name]
    collection = db[collection_name]
    return collection

def batch_aggregate_user_activity(collection, batch_size):
    pipeline = [
        {"$project": {"author": 1, "created_utc": 1}},  # Only project necessary fields
        {"$group": {
            "_id": {
                "author": "$author",
                "time_window": {
                    "$toDate": {
                        "$subtract": [
                            {"$toLong": "$created_utc"},
                            {"$mod": [{"$toLong": "$created_utc"}, 1000 * 60 * 60 * 24 * int(TIME_WINDOW[:-1])]}
                        ]
                    }
                }
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.time_window": 1}},
        {"$skip": 0},  # Skip is updated in each batch
        {"$limit": batch_size}
    ]
    skip = 0
    while True:
        pipeline[3]["$skip"] = skip
        batch = list(collection.aggregate(pipeline, allowDiskUse=True))
        if not batch:
            break
        yield batch
        skip += batch_size

def calculate_fluctuation_scores(data):
    # Convert to DataFrame
    df = pd.DataFrame(data)
    df['time_window'] = pd.to_datetime(df['_id'].apply(lambda x: x['time_window']))
    df['author'] = df['_id'].apply(lambda x: x['author'])
    df.drop(columns=['_id'], inplace=True)

    # Pivot the data to have authors as rows and time windows as columns
    pivot_df = df.pivot_table(index='author', columns='time_window', values='count', fill_value=0)

    # Calculate the standard deviation of post counts for each user
    fluctuation_scores = pivot_df.std(axis=1).to_frame(name='fluctuation_score')

    return fluctuation_scores

def plot_user_activity(data, author, save_dir="./step2/"):
    # Ensure the save directory exists
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    user_data = data[data['author'] == author]
    plt.figure(figsize=(12, 6))
    plt.plot(user_data['time_window'], user_data['count'], marker='o')
    plt.title(f"Activity Pattern for User: {author}")
    plt.xlabel("Time Window")
    plt.ylabel("Number of Posts/Comments")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, f"{author}_activity.png"))
    plt.close()

# Main Execution
submission_collection = connect_to_mongodb(DB_NAME, SUBMISSION_COLLECTION_NAME)
comment_collection = connect_to_mongodb(DB_NAME, COMMENT_COLLECTION_NAME)

# Process and Plot Data in Batches
for collection, collection_name in [(submission_collection, "submissions"), (comment_collection, "comments")]:
    print(f"Processing {collection_name} data...")
    for batch in tqdm(batch_aggregate_user_activity(collection, BATCH_SIZE)):
        fluctuation_scores = calculate_fluctuation_scores(batch)
        for author in fluctuation_scores.index:
            plot_user_activity(pd.DataFrame(batch), author)

print("Processing complete.")
