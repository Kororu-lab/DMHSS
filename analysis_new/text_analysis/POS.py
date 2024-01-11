from pymongo import MongoClient
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk import pos_tag
import nltk
from tqdm import tqdm
import os
import seaborn as sns
import matplotlib.pyplot as plt
from collections import Counter

# NLTK setup
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

# MongoDB connection setup
client = MongoClient()
db = client['reddit']
collections = ['relevant_submissions', 'relevant_comments']
batch_size = 500

def process_text(text):
    words = word_tokenize(text)
    return pos_tag(words)

def analyze_batch(docs, collection_name):
    pos_counts = Counter()
    for doc in docs:
        if not isinstance(doc, dict):
            continue  # Skip if the document is not a dictionary

        text = ""
        if 'submissions' in collection_name:
            title = doc.get('title', '')
            selftext = doc.get('selftext', '')
            text = title + ' ' + selftext
        elif 'comments' in collection_name:
            text = doc.get('body', '')

        if text.strip():
            user_grp = doc.get('user_grp', 'Unknown')
            subreddit_grp = doc.get('subreddit_grp', 'Unknown')
            for _, tag in process_text(text):
                key = (user_grp, subreddit_grp, tag)
                pos_counts[key] += 1
    return pos_counts

def aggregate_results(all_counts):
    aggregated_data = []
    for (user_grp, subreddit_grp, tag), count in all_counts.items():
        aggregated_data.append({
            'user_grp': user_grp,
            'subreddit_grp': subreddit_grp,
            'tag': tag,
            'count': count
        })
    return pd.DataFrame(aggregated_data)

def visualize_results(df, output_dir):
    pivot_df = df.pivot_table(index='tag', columns=['user_grp', 'subreddit_grp'], values='count', fill_value=0)
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_df, annot=True, cmap='viridis')
    plt.title('POS Tag Distribution')
    plt.ylabel('POS Tag')
    plt.xlabel('User Group and Subreddit Group')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'pos_distribution.png'))

# Main execution
output_dir = './output_visualizations/'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

total_counts = Counter()

for collection_name in collections:
    collection = db[collection_name]
    cursor = collection.find().batch_size(batch_size)
    
    for batch in tqdm(cursor, desc=f"Analyzing {collection_name}", total=collection.estimated_document_count() // batch_size):
        if isinstance(batch, dict):
            # Convert single document to a list for uniform processing
            batch = [batch]
        batch_counts = analyze_batch(batch, collection_name)
        total_counts.update(batch_counts)

# Aggregating and Visualizing Results
aggregated_data = aggregate_results(total_counts)
visualize_results(aggregated_data, output_dir)
