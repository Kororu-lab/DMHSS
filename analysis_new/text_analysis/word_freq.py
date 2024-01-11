from pymongo import MongoClient
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
from tqdm import tqdm
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# NLTK setup
nltk.download('punkt')
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

# MongoDB setup
client = MongoClient()
db = client['reddit']
collections = ['relevant_submissions', 'relevant_comments']
batch_size = 500

def preprocess_text(text):
    words = word_tokenize(text.lower())
    return [word for word in words if word.isalpha() and word not in stop_words]

def analyze_batch(docs):
    word_freq = {}
    for doc in docs:
        text = doc.get('title', '') + ' ' + doc.get('selftext', '') if 'submissions' in doc else doc.get('body', '')
        user_grp = doc.get('user_grp', 'Unknown')
        subreddit_grp = doc.get('subreddit_grp', 'Unknown')
        words = preprocess_text(text)
        for word in words:
            key = (user_grp, subreddit_grp, word)
            word_freq[key] = word_freq.get(key, 0) + 1
    return word_freq

output_dir = 'output_visualizations'
os.makedirs(output_dir, exist_ok=True)

for collection_name in collections:
    collection = db[collection_name]
    cursor = collection.find().batch_size(batch_size)
    total_word_freq = {}
    
    for batch in tqdm(cursor, desc=f"Analyzing {collection_name}", total=collection.estimated_document_count() // batch_size):
        batch_word_freq = analyze_batch(batch)
        for key, count in batch_word_freq.items():
            total_word_freq[key] = total_word_freq.get(key, 0) + count

    # Convert to DataFrame
    df = pd.DataFrame([(k[0], k[1], k[2], v) for k, v in total_word_freq.items()], columns=['User Group', 'Subreddit Group', 'Word', 'Frequency'])
    pivot_df = df.pivot_table(index='User Group', columns='Subreddit Group', values='Frequency', aggfunc='sum', fill_value=0)
    pivot_df['ALL'] = pivot_df.sum(axis=1)
    pivot_df.loc['ALL'] = pivot_df.sum(axis=0)

    # Save to CSV
    csv_file = os.path.join(output_dir, f'{collection_name}_word_freq.csv')
    pivot_df.to_csv(csv_file)

    # Plot
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_df, annot=True, cmap='viridis', fmt='g')
    plt.title(f'Word Frequency Matrix - {collection_name}')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{collection_name}_word_freq_matrix.png'))
