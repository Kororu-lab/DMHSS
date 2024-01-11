from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
import os
import matplotlib.pyplot as plt
import seaborn as sns
from gensim import corpora, models

# MongoDB connection setup
client = MongoClient()
db = client['reddit']
collection_names = ['relevant_submissions', 'relevant_comments']
batch_size = 500

# NLTK setup for text processing
nltk.download('punkt')
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

def preprocess_text(text):
    text = text.lower()
    text = re.sub(r'\W+', ' ', text)
    tokens = word_tokenize(text)
    tokens = [word for word in tokens if word not in stop_words]
    return tokens

def process_batch(docs, collection_name):
    for doc in docs:
        # Check if the document is in the expected format (i.e., a dictionary)
        if not isinstance(doc, dict):
            print(f"Unexpected document format: {type(doc)} - {doc}")
            continue

        text = ""
        if 'submissions' in collection_name:
            # For submissions, combine title and selftext
            title = doc.get('title', '')
            selftext = doc.get('selftext', '')
            text = title + ' ' + selftext
        elif 'comments' in collection_name:
            # For comments, use body
            text = doc.get('body', '')

        if text.strip():
            yield preprocess_text(text)

def perform_lda_analysis(preprocessed_texts):
    dictionary = corpora.Dictionary(preprocessed_texts)
    corpus = [dictionary.doc2bow(text) for text in preprocessed_texts]

    lda_model = models.LdaModel(corpus, num_topics=10, id2word=dictionary, passes=15)
    topics = lda_model.print_topics(num_words=5)
    for topic in topics:
        print(topic)

    return lda_model, corpus, dictionary

def visualize_results(lda_model, corpus, dictionary):
    for t in range(lda_model.num_topics):
        plt.figure(figsize=(16,10))
        plt.imshow(models.LdaModel.show_topic(lda_model, t, 200), interpolation='bilinear')
        plt.axis("off")
        plt.title("Topic #" + str(t))
        plt.savefig(f'topic_{t}.png')

def analyze_collection(collection_name):
    collection = db[collection_name]
    cursor = collection.find().batch_size(batch_size)
    preprocessed_texts = []

    for batch in tqdm(cursor, desc=f"Processing {collection_name}", total=collection.estimated_document_count() // batch_size):
        # Ensure 'batch' is a list of documents
        if isinstance(batch, dict):
            # If 'batch' is a single document, convert it into a list
            batch = [batch]

        preprocessed_texts.extend(process_batch(batch, collection_name))

    return perform_lda_analysis(preprocessed_texts)

# Main execution
output_dir = './output_visualizations/'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for collection_name in collection_names:
    lda_model, corpus, dictionary = analyze_collection(collection_name)
    visualize_results(lda_model, corpus, dictionary)
