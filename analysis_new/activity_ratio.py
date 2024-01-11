import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import pandas as pd

# Hyperparameters
DB_NAME = 'reddit'
COLLECTION_COMMENTS = 'relevant_comments'
COLLECTION_SUBMISSIONS = 'relevant_submissions'

# Establish MongoDB connection
client = MongoClient()
db = client[DB_NAME]

# Function to fetch data and calculate activity ratios
def calculate_activity_ratios(user_group, collection_comments, collection_submissions):
    if user_group == 'Otr':
        return pd.Series({'SW': 0, 'MH': 0, 'Otr': 1}, name=user_group)

    pipeline = [
        {'$match': {'user_grp': user_group}},
        {'$group': {
            '_id': '$author',
            'total': {'$sum': 1},
            'sw_count': {'$sum': {'$cond': [{'$eq': ['$subreddit_grp', 'SW']}, 1, 0]}},
            'mh_count': {'$sum': {'$cond': [{'$eq': ['$subreddit_grp', 'MH']}, 1, 0]}},
            'otr_count': {'$sum': {'$cond': [{'$eq': ['$subreddit_grp', 'Otr']}, 1, 0]}}
        }},
        {'$project': {
            'sw_ratio': {'$divide': ['$sw_count', '$total']},
            'mh_ratio': {'$divide': ['$mh_count', '$total']},
            'otr_ratio': {'$divide': ['$otr_count', '$total']}
        }}
    ]

    results = list(db[collection_comments].aggregate(pipeline))
    results.extend(list(db[collection_submissions].aggregate(pipeline)))

    if len(results) == 0:
        return pd.Series({'SW': 0, 'MH': 0, 'Otr': 0}, name=user_group)

    # Calculate average ratios
    avg_sw_ratio = sum([user['sw_ratio'] for user in results]) / len(results)
    avg_mh_ratio = sum([user['mh_ratio'] for user in results]) / len(results)
    avg_otr_ratio = sum([user['otr_ratio'] for user in results]) / len(results)

    return pd.Series({'SW': avg_sw_ratio, 'MH': avg_mh_ratio, 'Otr': avg_otr_ratio}, name=user_group)

# Calculate ratios for each user group
sw_ratios = calculate_activity_ratios('SW', COLLECTION_COMMENTS, COLLECTION_SUBMISSIONS)
mh_ratios = calculate_activity_ratios('MH', COLLECTION_COMMENTS, COLLECTION_SUBMISSIONS)
otr_ratios = calculate_activity_ratios('Otr', COLLECTION_COMMENTS, COLLECTION_SUBMISSIONS)

# Combine into a DataFrame
ratios_df = pd.DataFrame([sw_ratios, mh_ratios, otr_ratios])

# Visualization
plt.figure(figsize=(10, 8))
sns.heatmap(ratios_df, annot=True, cmap='coolwarm')
plt.title('Activity Ratios of User Groups Across Subreddit Groups')
plt.ylabel('User Group')
plt.xlabel('Subreddit Group')
plt.savefig('heatmap_relevant.png')
