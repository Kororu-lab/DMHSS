from pymongo import MongoClient
import nltk
from nltk.corpus import stopwords
import textstat
from collections import Counter
import pandas as pd
from tqdm import tqdm
import csv

# Ensure NLTK resources are available
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

# MongoDB connection setup
client = MongoClient()  # Connect to the default MongoDB instance running on localhost
db = client['reddit']  # Replace 'reddit' with your actual database name if different

# Collection names and parameters
submissions_collection_name = "filtered_submissions"  # Replace with your submissions collection name
comments_collection_name = "filtered_comments"  # Replace with your comments collection name
batch_size = 1000  # Adjust batch size based on your memory capacity and document size
expanded_research = False  # Set to True for expanded research as per your new requirements

# Define subreddits
SW = ['SuicideWatch']
MH_subreddits = [
    "depression", "mentalhealth", "traumatoolbox", "bipolarreddit", 
    "BPD", "ptsd", "psychoticreddit", "EatingDisorders", "StopSelfHarm", 
    "survivorsofabuse", "rapecounseling", "hardshipmates", 
    "panicparty", "socialanxiety"
] 

# Convert SW and MH_subreddits lists to lowercase
SW = [sub.lower() for sub in SW]
MH_subreddits = [sub.lower() for sub in MH_subreddits]

# Function for linguistic analysis
def process_text(text):
    sentences = nltk.sent_tokenize(text)
    words = nltk.word_tokenize(text)
    tagged = nltk.pos_tag(words)
    counts = Counter(tag for word, tag in tagged)

    num_nouns = sum(counts[tag] for tag in ['NN', 'NNS', 'NNP', 'NNPS'])
    num_verbs = sum(counts[tag] for tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ'])
    num_adverbs = sum(counts[tag] for tag in ['RB', 'RBR', 'RBS'])

    # Pronoun counts
    pronouns = {'first_person': 0, 'second_person': 0, 'third_person': 0}
    her_count = 0  # Count for the word 'her'
    for word, tag in tagged:
        if tag in ['PRP', 'PRP$']:
            word_lower = word.lower()
            if word_lower in ['i', 'me', 'my', 'mine', 'myself']:
                pronouns['first_person'] += 1
            elif word_lower in ['you', 'your', 'yours', 'yourself', 'yourselves']:
                pronouns['second_person'] += 1
            else:
                pronouns['third_person'] += 1

            if word_lower == 'her':
                her_count += 1

    readability_score = textstat.flesch_kincaid_grade(text)

    return {
        "num_sentences": len(sentences),
        "num_words": len(words),
        "num_nouns": num_nouns,
        "num_verbs": num_verbs,
        "num_adverbs": num_adverbs,
        "first_person_pronouns": pronouns['first_person'],
        "second_person_pronouns": pronouns['second_person'],
        "third_person_pronouns": pronouns['third_person'],
        "her_count": her_count,
        "readability_score": readability_score
    }

# Function to analyze a collection
def analyze_collection(collection_name):
    results = []
    user_posts = {}  # Dictionary to track user posts across categories

    cursor = db[collection_name].find().batch_size(batch_size)
    for post in tqdm(cursor, desc=f"Processing {collection_name}"):
        subreddit = str(post.get('subreddit', '')).lower()  # Ensure subreddit is a string
        user = post['author']
        text = post.get('body') or post.get('title') or post.get('selftext')

        # Update user_posts dictionary
        if user not in user_posts:
            user_posts[user] = {'SW': 0, 'MH': 0, 'Others': 0}
        if subreddit in SW:
            user_posts[user]['SW'] += 1
        elif subreddit in MH_subreddits:
            user_posts[user]['MH'] += 1
        else:
            user_posts[user]['Others'] += 1

        # Make sure text is a string
        if text and isinstance(text, str):
            analysis = process_text(text)

            # Determine user group based on posting behavior
            if expanded_research:
                if user_posts[user]['SW'] > 0:
                    user_group = 'SW'
                elif user_posts[user]['MH'] > 0:
                    user_group = 'MH'
                else:
                    user_group = 'Others'
            else:
                user_group = 'SW' if subreddit in SW else ('MH' if subreddit in MH_subreddits else 'Others')

            analysis['subreddit'] = subreddit
            analysis['user_group'] = user_group
            results.append(analysis)

    return results

# Analyze submissions and comments
submission_results = analyze_collection(submissions_collection_name)
comment_results = analyze_collection(comments_collection_name) if db[comments_collection_name].estimated_document_count() > 0 else []

# Combine and convert results to DataFrame
combined_results = submission_results + comment_results
df = pd.DataFrame(combined_results)

# Aggregate and analyze linguistic patterns
agg_df = df.groupby('user_group').agg({
    'num_nouns': 'mean',
    'num_verbs': 'mean',
    'num_adverbs': 'mean',
    'first_person_pronouns': 'mean',
    'second_person_pronouns': 'mean',
    'third_person_pronouns': 'mean',
    'readability_score': 'mean'
}).reset_index()

# Save the aggregated DataFrame to a CSV file
agg_df.to_csv("aggregated_linguistic_patterns.csv", index=False)

# Additional specific analyses and CSV generation can be added here
