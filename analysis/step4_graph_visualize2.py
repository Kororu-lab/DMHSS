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

def prepare_matrix_data(df, drop_zero):
    melted_df = df.melt(id_vars=['author', 'user_grp', 'year_month'], var_name='category', value_name='score')
    melted_df['score'] = pd.to_numeric(melted_df['score'], errors='coerce')
    
    if drop_zero:
        melted_df = melted_df[melted_df['score'] != 0]

    melted_df['subreddit_grp'] = melted_df['category'].str.split('-', expand=True)[0]
    pivot_table = melted_df.pivot_table(index='user_grp', columns='subreddit_grp', values='score', aggfunc='mean', fill_value=0)
    pivot_table = pivot_table.reindex(["SW", "MH", "Otr", "ALL"], axis=0)  # Fix row order
    return pivot_table

def prepare_time_data(df, drop_zero):
    melted_df = df.melt(id_vars=['author', 'user_grp', 'year_month'], var_name='category', value_name='score')
    melted_df['score'] = pd.to_numeric(melted_df['score'], errors='coerce')
    
    if drop_zero:
        melted_df = melted_df[melted_df['score'] != 0]

    category_split = melted_df['category'].str.split('-', expand=True)
    melted_df['subreddit_grp'] = category_split[0]
    melted_df['type'] = category_split[1] if category_split.shape[1] > 1 else 'unknown'

    melted_df['year_month'] = pd.to_datetime(melted_df['year_month'], format='%Y-%m', errors='coerce')
    grouped = melted_df.groupby(['user_grp', 'subreddit_grp', 'year_month'])['score'].mean().reset_index()
    return grouped

def create_matrix_plot(df):
    plt.figure(figsize=(10, 6))
    sns.heatmap(df, annot=True, fmt=".2f", cmap='viridis')
    plt.title("Average Scores by User Group and Subreddit Group")
    plt.ylabel("User Group")
    plt.xlabel("Subreddit Group")
    plt.savefig(os.path.join(plot_dir, "matrix_plot.png"))

def create_time_wise_plot(df):
    plt.figure(figsize=(15, 10))
    sns.lineplot(data=df, x='year_month', y='score', hue='user_grp', style='subreddit_grp', markers=True, dashes=False)
    plt.title("Average Scores Over Time by User Group and Subreddit Group")
    plt.ylabel("Average Score")
    plt.xlabel("Year-Month")
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(plot_dir, "time_wise_plot.png"))

def main(drop_zero=True):
    combined_data = merge_data([load_data(file, grp) for file, grp in zip(data_files, user_groups)])
    matrix_data = prepare_matrix_data(combined_data, drop_zero)
    create_matrix_plot(matrix_data)

    time_data = prepare_time_data(combined_data, drop_zero)
    if not time_data.empty:
        create_time_wise_plot(time_data)
    else:
        print("No data available for time-wise plot.")

if __name__ == "__main__":
    main(drop_zero=True)
    