from pymongo import MongoClient
import nltk
from nltk.corpus import stopwords
import textstat
from collections import Counter
from tqdm import tqdm
import csv
import os

# Ensure NLTK resources are available
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

# MongoDB connection setup
client = MongoClient()  # Connect to the default MongoDB instance running on localhost
db = client['reddit']  # Replace 'reddit' with your actual database name if different

# Collection names and parameters
submissions_collection_name = "filtered_submissions"  # Replace with your submissions collection name
comments_collection_name = "filtered_comments"  # Replace with your comments collection name
batch_size = 100  # Adjust batch size based on your memory capacity and document size
expanded_research = False  # Set to True for expanded research as per your new requirements

# Function for linguistic analysis
def process_text(text):
    sentences = nltk.sent_tokenize(text)
    words = nltk.word_tokenize(text)
    tagged = nltk.pos_tag(words)
    counts = Counter(tag for word, tag in tagged)

    num_nouns = sum(counts[tag] for tag in ['NN', 'NNS', 'NNP', 'NNPS'])
    num_verbs = sum(counts[tag] for tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ'])
    num_adverbs = sum(counts[tag] for tag in ['RB', 'RBR', 'RBS'])

    pronouns = {'first_person': 0, 'second_person': 0, 'third_person': 0}
    her_count = 0
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

# Function to analyze a collection and save results to CSV
def analyze_collection_and_save(collection_name, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['subreddit', 'subreddit_group', 'user_group', 'num_sentences', 'num_words', 
                         'num_nouns', 'num_verbs', 'num_adverbs', 'first_person_pronouns', 
                         'second_person_pronouns', 'third_person_pronouns', 
                         'her_count', 'readability_score'])

        cursor = db[collection_name].find().batch_size(batch_size)
        for post in tqdm(cursor, desc=f"Processing {collection_name}"):
            subreddit = post.get('subreddit', '')
            subreddit_grp = post.get('subreddit_grp', '').upper()
            user_grp = post.get('usr_grp', '').upper() if expanded_research else subreddit_grp
            text = post.get('body') or post.get('title') or post.get('selftext')

            if text and isinstance(text, str):
                analysis = process_text(text)
                writer.writerow([subreddit, subreddit_grp, user_grp,
                                 analysis['num_sentences'], analysis['num_words'],
                                 analysis['num_nouns'], analysis['num_verbs'],
                                 analysis['num_adverbs'], analysis['first_person_pronouns'],
                                 analysis['second_person_pronouns'], analysis['third_person_pronouns'],
                                 analysis['her_count'], analysis['readability_score']])

        # Clean up memory after processing each batch
        del cursor

# Ensure output directory exists
output_dir = "./step2/"
os.makedirs(output_dir, exist_ok=True)

# Analyze submissions and comments, saving results to CSV files
analyze_collection_and_save(submissions_collection_name, os.path.join(output_dir, "submission_results.csv"))
if db[comments_collection_name].estimated_document_count() > 0:
    analyze_collection_and_save(comments_collection_name, os.path.join(output_dir, "comment_results.csv"))

# Additional specific analyses and CSV generation can be added here
