import pymongo
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

# Hyperparameters
DB_NAME = "your_database_name"  # Replace with your database name
SUBMISSION_COLLECTION_NAME = "your_submission_collection_name"  # Replace with your submissions collection name
COMMENT_COLLECTION_NAME = "your_comment_collection_name"  # Replace with your comments collection name
TIME_WINDOW = "1D"  # Example: '1D' for daily, '1W' for weekly

def connect_to_mongodb(db_name, collection_name):
    # Connect to the MongoDB database and collection
    client = pymongo.MongoClient()  # Add connection string if not using default
    db = client[db_name]
    collection = db[collection_name]
    return collection

def aggregate_user_activity(collection):
    # Aggregate posts/comments per user over defined time windows
    pipeline = [
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
        {"$sort": {"_id.time_window": 1}}  # Sorting by time window
    ]
    return list(collection.aggregate(pipeline))

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

def plot_user_activity(data, author):
    user_data = data[data['author'] == author]
    plt.figure(figsize=(12, 6))
    plt.plot(user_data['time_window'], user_data['count'], marker='o')
    plt.title(f"Activity Pattern for User: {author}")
    plt.xlabel("Time Window")
    plt.ylabel("Number of Posts/Comments")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Main Execution
submission_collection = connect_to_mongodb(DB_NAME, SUBMISSION_COLLECTION_NAME)
comment_collection = connect_to_mongodb(DB_NAME, COMMENT_COLLECTION_NAME)

# Aggregating user activity
print("Aggregating submissions data...")
submission_data = aggregate_user_activity(submission_collection)
print("Aggregating comments data...")
comment_data = aggregate_user_activity(comment_collection)

# Combine and Calculate Fluctuation Scores
combined_data = submission_data + comment_data
print("Calculating fluctuation scores...")
fluctuation_scores = calculate_fluctuation_scores(combined_data)

# Plotting and Reporting
print("Generating plots...")
for user_data in tqdm(fluctuation_scores):
    plot_user_activity(user_data)

print("Processing complete.")
