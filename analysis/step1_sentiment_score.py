import pymongo
import csv
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm

# Database Configuration
DB_NAME = "reddit"
COLLECTION_SUBMISSIONS = "filtered_submissions_score2"
COLLECTION_COMMENTS = "filtered_comments_score2"

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]

# MongoDB Aggregation Pipeline
# Adjust the field names and logic according to your data schema
pipeline = [
    {"$match": {"author": {"$ne": "[deleted]"}}},
    {"$group": {
        "_id": "$author",
        "avg_sentiment": {"$avg": "$sentiment_score"}  # Use the correct field name
    }}
]

# Fetch Data from MongoDB
print("Fetching data from MongoDB...")
submissions_data = list(db[COLLECTION_SUBMISSIONS].aggregate(pipeline, allowDiskUse=True))
comments_data = list(db[COLLECTION_COMMENTS].aggregate(pipeline, allowDiskUse=True))

# Combining Submissions and Comments Data
combined_data = {}
for data in submissions_data + comments_data:
    author = data["_id"]
    avg_sentiment = data["avg_sentiment"]
    if author in combined_data:
        combined_data[author].append(avg_sentiment)
    else:
        combined_data[author] = [avg_sentiment]

# Calculate Final Average Sentiment for Each User
final_data = {author: sum(sentiments)/len(sentiments) for author, sentiments in combined_data.items()}

# Visualization
plt.figure(figsize=(10, 6))
sns.distplot(list(final_data.values()), bins=30, kde=False)
plt.title("Distribution of Average Sentiment Scores")
plt.xlabel("Average Sentiment Score")
plt.ylabel("Number of Users")
plt.savefig("sentiment_distribution.png")

# Save Data to CSV
with open("average_sentiment_scores.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Author", "Average Sentiment Score"])
    for author, avg_score in tqdm(final_data.items(), desc="Writing to CSV"):
        writer.writerow([author, avg_score])

print("Analysis complete. Data saved to CSV and plot generated.")
