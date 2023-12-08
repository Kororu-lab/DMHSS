import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from functools import reduce

# Directory where plots will be saved
plot_dir = "./step4/plots"
data_files = ["./step4/user_interaction_scores_SW.csv", "./step4/user_interaction_scores_MH.csv", "./step4/user_interaction_scores_Otr.csv"]
user_groups = ["SW", "MH", "Otr"]

# Ensure the directory exists
os.makedirs(plot_dir, exist_ok=True)

def load_data(file, user_grp):
    df = pd.read_csv(file)
    df['user_grp'] = user_grp
    year_month_col = df.columns.str.extract(r'(\d{4}-\d{2})')[0]
    df['year_month'] = year_month_col.ffill()
    return df

def merge_data(dfs):
    return reduce(lambda left, right: pd.merge(left, right, on=['author', 'year_month', 'user_grp'], how='outer'), dfs)

def prepare_matrix_data(df, category, drop_zero):
    melted_df = df.melt(id_vars=['author', 'user_grp', 'year_month'], var_name='category', value_name='score')
    melted_df['score'] = pd.to_numeric(melted_df['score'], errors='coerce')

    if drop_zero:
        melted_df = melted_df[melted_df['score'] != 0]

    melted_df['subreddit_grp'] = melted_df['category'].str.split('-', expand=True)[0]
    melted_df = melted_df[melted_df['category'].str.contains(category)]
    
    # Add 'ALL' column for average across all subreddit groups
    all_avg = melted_df.groupby(['user_grp', 'year_month']).agg({'score': 'mean'}).reset_index()
    all_avg['subreddit_grp'] = 'ALL'
    melted_df = pd.concat([melted_df, all_avg])

    pivot_table = melted_df.pivot_table(index='user_grp', columns='subreddit_grp', values='score', aggfunc='mean', fill_value=None)
    pivot_table = pivot_table.reindex(["SW", "MH", "Otr"], axis=0)  # Fix row order
    pivot_table = pivot_table.reindex(["SW", "MH", "Otr", "ALL"], axis=1)  # Fix column order
    return pivot_table

def create_matrix_plot(df, category):
    plt.figure(figsize=(10, 6))
    sns.heatmap(df, annot=True, fmt=".2f", cmap='viridis', cbar=True, linewidths=.5)
    plt.title(f"Average Scores by User Group and Subreddit Group - {category.capitalize()}")
    plt.ylabel("User Group")
    plt.xlabel("Subreddit Group")
    plt.savefig(os.path.join(plot_dir, f"matrix_plot_{category}.png"))

def main(drop_zero=True):
    combined_data = merge_data([load_data(file, grp) for file, grp in zip(data_files, user_groups)])
    
    # Create matrix and time-wise plots for each category
    for category in ['sub', 'com']:
        matrix_data = prepare_matrix_data(combined_data, category, drop_zero)
        create_matrix_plot(matrix_data, category)

    # For average, combine 'sub' and 'com' and then create the plots
    matrix_data_avg = (prepare_matrix_data(combined_data, 'sub', drop_zero) + prepare_matrix_data(combined_data, 'com', drop_zero)) / 2
    create_matrix_plot(matrix_data_avg, 'avg')

if __name__ == "__main__":
    main(drop_zero=True)
    