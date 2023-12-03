import csv
import numpy as np
from pymongo import MongoClient
from collections import defaultdict
from tqdm import tqdm

### Hyperparameters and Configurations ###
DB_NAME = "reddit"
OUTPUT_FILE = "low500_suicide2_full.csv"
RANK_USER_OUTPUT_FILE = "rank_user_lst.csv"
ORDER = "ascending"  # Can be "ascending" or "descending"

# MongoDB Collections
COLLECTION_SUBMISSIONS = "filtered_submissions_score2"
COLLECTION_COMMENTS = "filtered_comments_score2"

# MongoDB Connection
client = MongoClient()
db = client[DB_NAME]

# Define normalization ranges and weights for integration
norm_ranges = {
    'sentiment': (0, 1),
    'pos': (0, 1),
    'graph': (0, 1),
    'time_based': (0, 1)
}

weights = {
    'sentiment': 0.25,
    'pos': 0.25,
    'graph': 0.25,
    'time_based': 0.25
}

# Functions for score normalization and integration
def normalize_score(score, score_type):
    """ Normalize the score to a 0-1 range """
    min_score, max_score = norm_ranges[score_type]
    return (score - min_score) / (max_score - min_score)

def integrate_scores(user_data):
    """ Calculate the integrated score for a user """
    integrated_score = sum(weights[type] * normalize_score(user_data[type], type) for type in weights)
    return integrated_score

# Process and integrate user data
users_scores = defaultdict(lambda: defaultdict(list))  # Stores raw scores for each user
users_integrated_scores = {}  # Stores integrated scores for each user

# Fetch data from MongoDB
cursor_submissions = db[COLLECTION_SUBMISSIONS].find()
cursor_comments = db[COLLECTION_COMMENTS].find()

# Aggregate user data
for doc in tqdm(cursor_submissions, desc="Processing Submissions"):
    users_scores[doc["author"]]["submission"].append(doc)
for doc in tqdm(cursor_comments, desc="Processing Comments"):
    users_scores[doc["author"]]["comment"].append(doc)

# Calculate integrated scores for each user
for user, activities in tqdm(users_scores.items(), desc="Integrating Scores"):
    # Example data aggregation (modify as per actual score types and logic)
    avg_sentiment = np.mean([d['sentiment_score'] for d in activities['submission'] + activities['comment']])
    avg_pos = np.mean([d.get('pos_score', 0) for d in activities['submission'] + activities['comment']])  # Replace with actual POS score
    avg_graph = np.mean([d.get('graph_score', 0) for d in activities['submission'] + activities['comment']])  # Replace with actual graph score
    avg_time_based = np.mean([d.get('time_based_score', 0) for d in activities['submission'] + activities['comment']])  # Replace with actual time-based score

    user_data = {
        'sentiment': avg_sentiment,
        'pos': avg_pos,
        'graph': avg_graph,
        'time_based': avg_time_based
    }
    integrated_score = integrate_scores(user_data)
    users_integrated_scores[user] = integrated_score

# Sort users by their integrated scores
sorted_users = sorted(users_integrated_scores.items(), key=lambda x: x[1], reverse=(ORDER == "descending"))

# Write detailed activities to CSV
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["User", "Integrated_Score", "Sentiment", "POS", "Graph", "Time_Based"])  # Define your headers
    for user, score in sorted_users:
        writer.writerow([user, score, user_data['sentiment'], user_data['pos'], user_data['graph'], user_data['time_based']])

# Write rank and user list to CSV
with open(RANK_USER_OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Rank", "User"])
    for rank, (user, _) in enumerate(sorted_users, start=1):
        writer.writerow([rank, user])

# Close MongoDB connection
client.close()

print(f"Finished generating {OUTPUT_FILE} and {RANK_USER_OUTPUT_FILE}.")
