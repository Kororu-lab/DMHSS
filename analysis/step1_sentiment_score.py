import pymongo
import csv
import matplotlib.pyplot as plt
import os
from collections import defaultdict
from tqdm import tqdm

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
OUTPUT_DIR = "./step1/"
BATCH_SIZE = 100  # Adjustable batch size for data processing

# Collection names
COLLECTION_SUBMISSIONS = "filtered_submissions_sentiment"
COLLECTION_COMMENTS = "filtered_comments_sentiment"

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# MongoDB connection
client = pymongo.MongoClient()
db = client[DB_NAME]

# Function to process data from MongoDB
def process_data(collection, score_field, desc):
    pipeline = [
        {"$match": {"author": {"$ne": "[deleted]"}}},
        {"$project": {"author": 1, score_field: 1, "subreddit_grp": 1, "user_grp": 1}},
        {"$group": {
            "_id": {"author": "$author", "subreddit_grp": "$subreddit_grp", "user_grp": "$user_grp"},
            "avg_sentiment": {"$avg": f"${score_field}"}
        }}
    ]
    cursor = db[collection].aggregate(pipeline, allowDiskUse=True, batchSize=BATCH_SIZE)
    data_list = []
    for data in tqdm(cursor, desc=desc):
        data_list.append(data)
    return data_list

# Process submissions and comments with different sentiment score fields
print("Processing submissions...")
submissions_data = process_data(COLLECTION_SUBMISSIONS, "sentiment_score_complex", "Processing Submissions")
print("Processing comments...")
comments_data = process_data(COLLECTION_COMMENTS, "sentiment_score", "Processing Comments")

# Combine data
print("Combining data...")
user_scores = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
for data in submissions_data + comments_data:
    author = data["_id"]["author"]
    subreddit_grp = data["_id"]["subreddit_grp"]
    user_grp = data["_id"]["user_grp"]
    user_scores[author][user_grp][subreddit_grp].append(data["avg_sentiment"])

# Calculate final average sentiment scores
print("Calculating final scores...")
final_scores = {}
for author, user_grps in user_scores.items():
    final_scores[author] = {user_grp: {subreddit_grp: sum(scores) / len(scores) if scores else 0 for subreddit_grp, scores in subreddits.items()} for user_grp, subreddits in user_grps.items()}

# Save the results to CSV and plot
print("Saving results...")
for user_grp in ["SW", "MH", "Otr"]:
    for subreddit_grp in ["SW", "MH", "Otr"]:
        filename = OUTPUT_DIR + f"sentiment_scores_{user_grp}_{subreddit_grp}.csv"
        with open(filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["User", "Average Sentiment Score"])
            for author, user_grps in tqdm(final_scores.items(), desc=f"Writing {user_grp}-{subreddit_grp}"):
                if user_grp in user_grps and subreddit_grp in user_grps[user_grp]:
                    score = user_grps[user_grp][subreddit_grp]
                    writer.writerow([author, score])

        # Plot sentiment score distributions
        scores = [user_grps[user_grp][subreddit_grp] for author, user_grps in final_scores.items() if user_grp in user_grps and subreddit_grp in user_grps[user_grp]]
        plt.figure()
        plt.hist(scores, bins=30, alpha=0.7, label=f"{user_grp}-{subreddit_grp} Sentiment Scores")
        plt.title(f'Sentiment Score Distribution: {user_grp} Users in {subreddit_grp}')
        plt.xlabel('Average Sentiment Score')
        plt.ylabel('Frequency')
        plt.legend()
        plt.savefig(OUTPUT_DIR + f"sentiment_distribution_{user_grp}_{subreddit_grp}.png")

print("Analysis complete. Data and plots saved to the './step1/' directory.")
