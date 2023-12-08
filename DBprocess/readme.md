## ./DBProcess

This part used for filtering existing database or extracting some information from large dataset. 

### Main Scripts
DBfilter_bysubreddit_*: filtering data for given collections, with only that has matching subreddit given.

DBfilter_byuserID_*: filering by given user ID list as CSV.

Extract_user.py: From given collection extract the whole user list, could be used to DBfilter_byuserID.

txt files: containing commands for mongosh.