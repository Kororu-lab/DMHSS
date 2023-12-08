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
#### POS Usage
It was found that users who moved to SW from MH tend to use less ratio of nouns while more verbs, adverbs.
#### Pronoun Usage
It was found that users who moved to SW from MH more likely to use 1st singular pronouns(i.e. 'I') and 3rd person pronouns(i.e. 'he') more then others. 
#### Frequent Word type
According to the paper it was found users who moved from MH to SW tend to use particular words relatively more often then who not moved to SW.
## Integrated Model
To overcome the previous problem of single post sentiment based models, we tried to get complex features from all-across reddit archive and integrate in single model to find out people who are in severe status effectively. To achieve the goal we tried various analysis methods for given data and found out some useful features to integrate.
## Analysis Methods We used
We integrated four metrics to analyze each users active patterns with methods below:
### Sentiment Score
This score metrics corresponds to existing metrics with normal sentiment analysis. However, we tried to give each activity a sentimetal temperature and tried to ditinguish and track for their timeline to get sentimantal delta.
#### Detailed Metrics(Temp ver.)
### Language Structure Score
Based on the 2016 studies of Reddit users we tried to check if those also applied for other types of users, and tried to integrate those lingual structures informations in the model of language structures, including POS, pronouns, keywords, text length, and so on.
#### Detailed Metrics(Temp ver.)
### Active Pattern Score
To figure out if users show different patterns of activity throughout the reddit, we tried to make time-window methods to numeralize those in degree of danger.
#### Detailed Metrics(Temp ver.)
### Communication Score
It is well known that those who are well going with communication with others has less danger of suicidal decision/execution, opposite not. Based on this idea we tried to make scores that could represent each users' degrees of commiting suicide.
#### Detailed Metrics(Temp ver.)

Due to the lack of computation, we constrainted our methods into 4, and integrated by summ up with weights for each normalized scores.

## Model test
To test our model performance we tried some competetion for our new model and previous sentiment-based models, to make rank of SW users, and get 50 users with most severe status(according to each model), then got their all activity from whole reddit(see /analysis/Extraction.py). Detailed rules is as below:
### (TBD)
### Result

## Structure of the Repo
This repo contains all code files that made from performing the project, except only that contains particularly sensitive information or too large to upload. We run the code from following local machine so it could contain some locale dir that optimized to.
### About Machine
Our machine run at local with following specifics:

OS: Ubuntu server 20.04(LTS)

CPU: intel i9-9900k

Memory/Swap: 64G DDR4/128G SSD(part of root disk) each

Storage: 6.4T for root(/), mounted disk 3.84T(SSD1), 2T(SSD2), 1T(SSD3), 16T(HDD1), 6T(HDD2)

GPU: NVIDIA RTX 4090(24G VRAM)

### Directory

#### ./data
This part of repo consists of processing Reddit Archive datasets.
#### ./DBProcess
This part used for filtering existing database or extracting some information from large dataset. 
#### ./models
This directory is purely for processing with sentiment models from HuggingFace.
#### ./analysis
Various Analysis methods used & outcomes
#### Others
