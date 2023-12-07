from pymongo import MongoClient
import pandas as pd
import random
import os
from tqdm import tqdm
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.tag import pos_tag
import textstat
from collections import Counter

# Download necessary NLTK resources
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

# MongoDB connection setup
client = MongoClient()
db = client['reddit']

# Collection names and parameters
submissions_collection_name = "stat_submissions"
comments_collection_name = "stat_comments"
output_dir = "./step2/stats"
batch_size = 1000  # Adjust based on memory capacity
sample_size = 250000  # Number of samples to take for each group

# Define valid combinations for analysis
valid_combinations = {
    ('SW', 'SW'), 
    ('SW', 'MH'), 
    ('SW', 'Otr'), 
    ('MH', 'MH'), 
    ('MH', 'Otr'), 
    ('Otr', 'Otr')
}

# Define critical keywords
critical_keywords = ["depression", "suicide", "anxiety", "suicidal", "can't"]

def process_text(text):
    text = str(text) if text is not None else ''
    sentences = sent_tokenize(text)
    words = word_tokenize(text)
    tagged = pos_tag(words)
    pos_counts = Counter(tag for word, tag in tagged)
    text_length = len(words)

    num_nouns = sum(pos_counts[tag] for tag in ['NN', 'NNS', 'NNP', 'NNPS'])
    num_verbs = sum(pos_counts[tag] for tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ'])
    num_adverbs = sum(pos_counts[tag] for tag in ['RB', 'RBR', 'RBS'])

    pronouns = {'first_person_singular': 0, 'first_person_plural': 0, 'second_person': 0, 'third_person': 0}
    for word, tag in tagged:
        word_lower = word.lower()
        if tag in ['PRP', 'PRP$']:
            if word_lower in ['i', 'me', 'my', 'mine', 'myself']:
                pronouns['first_person_singular'] += 1
            elif word_lower in ['we', 'us', 'our', 'ours', 'ourselves']:
                pronouns['first_person_plural'] += 1
            elif word_lower in ['you', 'your', 'yours', 'yourself', 'yourselves']:
                pronouns['second_person'] += 1
            else:
                pronouns['third_person'] += 1

    readability_score = textstat.flesch_kincaid_grade(text)
    keyword_score = any(word in text.lower() for word in critical_keywords)

    # Calculate normalized metrics
    total_words = len(words)
    noun_ratio = num_nouns / total_words if total_words > 0 else 0
    verb_ratio = num_verbs / total_words if total_words > 0 else 0
    adverb_ratio = num_adverbs / total_words if total_words > 0 else 0

    # Calculate total counts for each pronoun category
    total_pronouns = pronouns['first_person_singular'] + pronouns['first_person_plural'] + \
                     pronouns['second_person'] + pronouns['third_person']
    

    # Calculate ratios of each pronoun category relative to total pronouns
    first_person_singular_ratio = pronouns['first_person_singular'] / total_pronouns if total_pronouns > 0 else 0
    first_person_plural_ratio = pronouns['first_person_plural'] / total_pronouns if total_pronouns > 0 else 0
    second_person_ratio = pronouns['second_person'] / total_pronouns if total_pronouns > 0 else 0
    third_person_ratio = pronouns['third_person'] / total_pronouns if total_pronouns > 0 else 0

    return {
        "num_nouns": num_nouns,
        "num_verbs": num_verbs,
        "num_adverbs": num_adverbs,
        "first_person_singular_pronouns": pronouns['first_person_singular'],
        "first_person_plural_pronouns": pronouns['first_person_plural'],
        "second_person_pronouns": pronouns['second_person'],
        "third_person_pronouns": pronouns['third_person'],
        # Normalized metrics
        "noun_ratio": noun_ratio,
        "verb_ratio": verb_ratio,
        "adverb_ratio": adverb_ratio,
        "first_person_singular_ratio": first_person_singular_ratio,
        "first_person_plural_ratio": first_person_plural_ratio,
        "second_person_ratio": second_person_ratio,
        "third_person_ratio": third_person_ratio,
        "readability_score": readability_score,
        "keyword_score": keyword_score,
        "text_length": text_length
    }

def sample_and_save_raw_data(collection_name, valid_combinations, output_dir, sample_size, batch_size):
    for (user_grp, subreddit_grp) in valid_combinations:
        n_samples = sample_size  # Use the specified sample size for each group combination
        output_path = f'{output_dir}/{collection_name}_{user_grp}_{subreddit_grp}_raw.csv'

        if os.path.exists(output_path):
            print(f"Raw data file {output_path} already exists. Skipping sampling.")
            continue

        sampled_docs = []
        progress = tqdm(total=n_samples, desc=f"Sampling {user_grp}/{subreddit_grp} in {collection_name}")

        while len(sampled_docs) < n_samples:
            cursor = db[collection_name].find({'user_grp': user_grp, 'subreddit_grp': subreddit_grp}).limit(batch_size)
            batch = list(cursor)
            if not batch:
                break  # No more documents to sample
            sample_count = min(len(batch), n_samples - len(sampled_docs))
            sampled_docs.extend(random.sample(batch, sample_count))
            progress.update(sample_count)

        df = pd.DataFrame(sampled_docs)
        df.to_csv(output_path, index=False)
        progress.close()
        print(f"Saved raw data to {output_path}")

def batch_process_and_analyze(collection_name, valid_combinations, output_dir, batch_size):
    for (user_grp, subreddit_grp) in valid_combinations:
        input_path = f'{output_dir}/{collection_name}_{user_grp}_{subreddit_grp}_raw.csv'
        output_path = f'{output_dir}/{collection_name}_{user_grp}_{subreddit_grp}_analysis.csv'

        if not os.path.exists(input_path):
            print(f"Raw data file {input_path} does not exist. Skipping analysis.")
            continue

        try:
            df = pd.read_csv(input_path, chunksize=batch_size)
        except pd.errors.EmptyDataError:
            print(f"No data in file {input_path}. Skipping analysis.")
            continue

        results = []
        progress_bar = tqdm(desc=f'Analyzing {user_grp}/{subreddit_grp} in {collection_name}')
        for batch in df:
            for _, doc in batch.iterrows():
                if collection_name.endswith('comments'):
                    text = str(doc.get('body', ''))
                else:  # For submissions, concatenate title and selftext
                    title = str(doc.get('title', ''))
                    selftext = str(doc.get('selftext', ''))
                    text = title + ' ' + selftext if selftext not in ['[removed]', '[deleted]'] else title

                if text.strip():
                    analysis = process_text(text)
                    analysis['user_grp'] = user_grp
                    analysis['subreddit_grp'] = subreddit_grp
                    results.append(analysis)
            progress_bar.update(len(batch))
        progress_bar.close()

        final_df = pd.DataFrame(results)
        final_df.to_csv(output_path, index=False)
        print(f"Saved analysis results to {output_path}")

# Main execution
sample_and_save_raw_data(submissions_collection_name, valid_combinations, output_dir, sample_size, batch_size)
sample_and_save_raw_data(comments_collection_name, valid_combinations, output_dir, sample_size, batch_size)

batch_process_and_analyze(submissions_collection_name, valid_combinations, output_dir, batch_size)
batch_process_and_analyze(comments_collection_name, valid_combinations, output_dir, batch_size)
