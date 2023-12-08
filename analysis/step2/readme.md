## Step 2 Analysis: Sentence Structure

Idea from 2016 studies of github, tried to figure out if those users shows different pattern for each sentence structures. Mainly about:

### POS Usage Ratio
For POS usage(noun/verb/adv.) It has shown that users of SW and MH tends to use less nouns while more verbs/adverbs. However, for SW users they used their texts neatly Otr group people, in some significant degree.

### Pronoun Usage Ratio
Notable high usage of 1st person singular found from SW and MH users. However, unlike those from papers, it didnt shown if those users use more 3rd person pronouns or not.

### Text Length Ratio
This part, rather then difference of user group, it seems some factor of subreddit group affects more. used longer texts with MH>SW>Otr order

### Keyword Usage Counts
From the paper we chose 5 main keywords:

"depression", "suicide", "anxiety", "suicidal", "can't"

It has shown there's some notable difference between subreddit group not users.

### Final Score
After analyze we decided to use "noun_ratio", "first_person_singular_ratio", "keyword_score", "text_length" for our final score for sentence structure. could find out plots from the analyzements of: "final_*.png" Also, each seections compare matrix plots available from "./visualizations".