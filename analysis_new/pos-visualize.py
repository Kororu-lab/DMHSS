import pandas as pd
import matplotlib.pyplot as plt
import os

def load_data(file_path):
    return pd.read_csv(file_path)

def plot_data(data, column, title, file_name):
    plt.figure(figsize=(10, 6))
    data[column].value_counts().plot(kind='bar')
    plt.title(title)
    plt.ylabel('Frequency')
    plt.xlabel(column)
    plt.savefig(f'./visualization/{file_name}.png')
    plt.close()

def generate_visualizations(csv_file):
    data = load_data(csv_file)
    # Plot POS tag frequencies
    plot_data(data, 'POS Tag', 'POS Tag Frequencies', 'pos_tag_freq')
    # Plot Word frequencies
    plot_data(data, 'Word', 'Word Frequencies', 'word_freq')
    # Add more plots as needed

os.makedirs('./visualization', exist_ok=True)
generate_visualizations('aggregated_submissions.csv')
generate_visualizations('aggregated_comments.csv')
