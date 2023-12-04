# DMHSS
Discreet Mental Health Support System (DMHSS)

Our primary goal is to construct integrated model to perform spot out people in danger of suicide. For the goal the models analyze full reddit archive data from https://www.reddit.com/r/DataHoarder/comments/1479c7b/historic_reddit_archives_ongoing_archival_effort/?rdt=43321, especially focusing on r/SuicideWatch user activity and some mental health related subreddits. 

## Previous Studies
Our project based on previous sentiment-based suicidal-detct models from Kaggle project "Suicide and Depression Detection"(https://www.kaggle.com/datasets/nikhileswarkomati/suicide-watch). It gave dataset of two subreddit "r/SuicideWatch", "r/depression" submission data and labeled them as T/F of suicidal danger. With this dataset and idea we first tried to upgrade the performance the model but found that these kind of approach has limit in their essens,as below:
### Problem I: Lack of Theoritical Basis of Classification.
The Kaggle project, to classify reddit user by their subreddit only, could make error since those submission data could not generalize full users who in danger of suicide. In r/SuicideWatch could be some people not in danger, while r/depression could contain some people who are in severe condition. It could only find out if the post from SW or outer, not the user danger of suicidal.
### Problem II: Lack of External Information or Metadata
This kind of method, to classify T/F by submission text only, could not get full features of people who trying to end his life. For mor
### SNS user research(2016)
Our further model partly refur to the method from 2016 research named "" which took a deep look into users of MH(Mental Health related subreddits) who moved to SW or not. They found out various user features with statistical method, including followings:
#### Pronoun Usage
#### Frequent Word type
## Integrated Model
To overcome the previous problem of single post sentiment based models, we tried to get complex features from all-across reddit archive and integrate in single model to find out people who are in severe status effectively. To achieve the goal we tried various analysis methods for given data and found out some useful features to integrate.
## Analysis Methods We used
We integrated four metrics to analyze each users active patterns with methods below:
### Sentiment Score
### Language Structure Score
### Active Pattern Score
### Communication Score

Due to the lack of computation, we constrainted our methods into 4, and integrated by summ up with weights for each normalized scores.

## Model test
To test our model performance we tried some competetion for our new model and previous sentiment-based models, to make rank of SW users, and get 50 users with most severe status(according to each model), then got their all activity from whole reddit(see /analysis/Extraction.py). Detailed rules is as below:
### (TBD)
### Result

