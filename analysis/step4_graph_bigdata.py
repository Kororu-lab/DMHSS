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

def process_comments_batch(G, batch, submissions_data):
    """ Process a batch of comments and update the graph """
    for comment in batch:
        author = comment['author']
        parent_id = process_id(comment['parent_id'])
        link_id = process_id(comment['link_id'])

        if parent_id in submissions_data:
            G.add_edge(author, submissions_data[parent_id]['author'], type='submission_reply')
        elif parent_id != link_id:
            G.add_edge(author, parent_id, type='comment_reply')
        if link_id in submissions_data:
            G.add_edge(author, submissions_data[link_id]['author'], type='submission_reference')

def process_comments_in_batches(G, comments_cursor, submissions_data, batch_size):
    """ Process comments in batches and add edges to the graph """
    batch = []
    for comment in comments_cursor:
        batch.append(comment)
        if len(batch) >= batch_size:
            process_comments_batch(G, batch, submissions_data)
            batch.clear()

    if batch:
        process_comments_batch(G, batch, submissions_data)

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

def main(batch_size=10000):
    # MongoDB Local Connection
    client = MongoClient()
    db = client['reddit']

    # Create directory for output
    output_dir = "./step4/"
    create_directory(output_dir)

    # Projection to load only necessary fields
    projection_comments = {'author': 1, 'parent_id': 1, 'link_id': 1}
    projection_submissions = {'author': 1, 'id': 1}

    # Extract Data from MongoDB
    print("Loading Data from MongoDB...")
    submissions_data = {doc['id']: doc for doc in tqdm(db['filtered_submissions'].find(projection=projection_submissions), desc="Loading Submissions")}

    # Check if graph file exists
    graph_file = os.path.join(output_dir, 'reddit_graph.gexf')
    if os.path.exists(graph_file):
        print("Loading existing graph...")
        G = nx.read_gexf(graph_file)
    else:
        # Create a Graph
        G = nx.DiGraph()
        print("Building the Graph in Batches...")

        # Process comments in batches
        comments_cursor = db['filtered_comments'].find(projection=projection_comments)
        process_comments_in_batches(G, comments_cursor, submissions_data, batch_size)

        # Save the graph to file
        nx.write_gexf(G, graph_file)

    # Calculate and Save Centrality Scores
    save_centrality_scores(G, output_dir)

    # Export Graph Visualization
    plt.figure(figsize=(10, 10))
    nx.draw(G, with_labels=False)
    plt.savefig(os.path.join(output_dir, 'graph_visualization.png'))

    print("Analysis completed.")

if __name__ == "__main__":
    main(batch_size=5000)  # Adjust batch size as needed for memory efficiency
