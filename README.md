# QonTwitterIDDP

This repo contains replication files for IDDP's report about QAnon on Twitter.

### Data
In compliance with Twitter's "Content redistribution" guidelines in the [Developer Policy](https://developer.twitter.com/en/developer-terms/policy), we provide a list of the ID of each tweet in our dataset ([link to file](https://osf.io/jxbgm/)). Researchers should be able to use this information to rehydrate tweets.

### Code
In addition, we provide a number of Python files that contain the code we used to extract data from our database, then process, analyze, and visualize that data.  

To ensure compliance with Twitter's guidelines, we cannot provide intermediate datafiles that we created as part of our extract-process-analyze-visualize pipeline. However, researchers should be able to use the code in these files to replicate this analysis using any dataset of hydrated tweets.  
1) language_breakdown_check.py: this file is used to create the chart showing the relative proportion of different languages.
2) daily_count.py: this file is used to generate the descriptive statistics and visualizations about the number of tweets per day.
3) confirm_mongo_account_info_w_id.py: this file is used to generate the descriptive statistics and visualizations about:
    - the number of users in the dataset
    - the number of tweets per user
    - the number of unique user descriptions per user
4) tweet_type_count.py: this file is used to count the number of retweets, quote tweets, and original tweets.
5) get_retwets_w_userid.py: this file is used to generate the descriptive statistics and visualizations about:
    - the number of times each tweet was retweeted
    - the number of times each account was retweeted
    - the number of times each account created retweets
    - the number of times accounts retweeted tweets originally sent by themselves
6) user_account_create_date.py: this file is used to generate the descriptive statistics and visualizations about when accounts that appear in our dataset were created.
7) user_bio_token_count.py: this file is used to generate the visualizations about the top unigrams, bigrams, and @mentions in user bios.
8) process_tweet_text.py: this file is used to generate the visualizations about the top unigrams and bigrams in the text of tweets.
