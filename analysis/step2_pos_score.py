from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.tag import pos_tag
from collections import Counter
import textstat

# Download necessary NLTK resources
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

# MongoDB connection setup
client = MongoClient()
db = client['reddit']

# Collection names and parameters
submissions_collection_name = "filtered_submissions"
comments_collection_name = "filtered_comments"
batch_size = 1000  # Adjust based on your MongoDB server's capability

# Weights for scoring
weights = {
    'noun_ratio': -1.5,
    'first_person_singular_ratio': 1,
    'keyword_score': 2,
    'text_length': 0.005
}

# Define critical keywords
critical_keywords = ["depression", "suicide", "anxiety", "suicidal", "can't"]

def process_text(text):
    text = str(text) if text is not None else ''
    words = word_tokenize(text)
    tagged = pos_tag(words)
    pos_counts = Counter(tag for word, tag in tagged)
    total_words = len(words)

    # Extract required features
    num_nouns = sum(pos_counts[tag] for tag in ['NN', 'NNS', 'NNP', 'NNPS'])
    first_person_singular_pronouns = sum(1 for word, tag in tagged if word.lower() in ['i', 'me', 'my', 'mine', 'myself'] and tag in ['PRP', 'PRP$'])
    keyword_score = any(word in text.lower() for word in critical_keywords)

    # Calculate ratios
    noun_ratio = num_nouns / total_words if total_words > 0 else 0
    first_person_singular_ratio = first_person_singular_pronouns / total_words if total_words > 0 else 0

    return {
        "noun_ratio": noun_ratio,
        "first_person_singular_ratio": first_person_singular_ratio,
        "keyword_score": keyword_score,
        "text_length": total_words
    }

def calculate_score(features, weights):
    # Function to calculate the final score based on the extracted features and weights
    # Efficiently calculates only for non-zero weights
    final_score = 0
    for feature, weight in weights.items():
        if weight != 0 and feature in features:
            final_score += weight * features[feature]
    return final_score

def update_collection_with_scores(collection_name):
    collection = db[collection_name]
    cursor = collection.find().batch_size(batch_size)
    
    for document in tqdm(cursor, desc=f"Processing {collection_name}"):
        # Handle submissions and comments differently
        if collection_name.endswith('submissions'):
            title = str(document.get('title', ''))
            selftext = str(document.get('selftext', ''))
            text = title + ' ' + selftext if selftext not in ['[removed]', '[deleted]'] else title
        else:  # For comments
            text = str(document.get('body', ''))

        if text.strip():
            features = process_text(text)
            score = calculate_score(features, weights)
            collection.update_one({'_id': document['_id']}, {'$set': {'pos_score': score}})

# Main execution
update_collection_with_scores(submissions_collection_name)
update_collection_with_scores(comments_collection_name)
