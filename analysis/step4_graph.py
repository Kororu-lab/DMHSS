import os
import pymongo
from pymongo import MongoClient
import networkx as nx
import matplotlib.pyplot as plt
from tqdm import tqdm

def create_directory(path):
    """ Create directory if it doesn't exist """
    if not os.path.exists(path):
        os.makedirs(path)

def process_id(reddit_id):
    """ Process Reddit ID to remove prefix """
    return reddit_id.split('_')[-1]

def save_centrality_scores(G, filename):
    """ Calculate and save centrality scores """
    centrality_funcs = {
        'degree': nx.degree_centrality,
        'betweenness': nx.betweenness_centrality,
        'closeness': nx.closeness_centrality
    }
    for name, func in centrality_funcs.items():
        centrality = func(G)
        with open(os.path.join(filename, f'{name}_centrality.txt'), 'w') as file:
            for user, score in centrality.items():
                file.write(f"{user}: {score}\n")

def main():
    # MongoDB Local Connection
    client = MongoClient()
    db = client['reddit']
    submissions = db['filtered_submissions']
    comments = db['filtered_comments']

    # Create directory for output
    output_dir = "./step4/"
    create_directory(output_dir)

    # Extract Data from MongoDB
    print("Loading Data from MongoDB...")
    submissions_data = {doc['id']: doc for doc in tqdm(submissions.find(), desc="Loading Submissions")}
    comments_data = list(tqdm(comments.find(), desc="Loading Comments"))

    # Create a Graph
    G = nx.DiGraph()
    print("Building the Graph...")
    for comment in tqdm(comments_data, desc="Processing Comments"):
        author = comment['author']
        parent_id = process_id(comment['parent_id'])
        link_id = process_id(comment['link_id'])

        if parent_id in submissions_data:
            G.add_edge(author, submissions_data[parent_id]['author'], type='submission_reply')
        elif parent_id != link_id:
            G.add_edge(author, parent_id, type='comment_reply')
        if link_id in submissions_data:
            G.add_edge(author, submissions_data[link_id]['author'], type='submission_reference')

    # Calculate and Save Centrality Scores
    save_centrality_scores(G, output_dir)

    # Export Graph Visualization
    plt.figure(figsize=(10, 10))
    nx.draw(G, with_labels=False)
    plt.savefig(os.path.join(output_dir, 'graph_visualization.png'))

    print("Analysis completed.")

if __name__ == "__main__":
    main()
