# QonTwitterIDDP

This repo contains replication files for IDDP's report about QAnon on Twitter.

### Data
In compliance with Twitter's "Content redistribution" guidelines in the [Developer Policy](https://developer.twitter.com/en/developer-terms/policy), we provide a list of the ID of each tweet in our dataset ([link to file](https://osf.io/jxbgm/)). Researchers should be able to use this information to rehydrate tweets (i.e., go from a list of tweet IDs to a list of full tweet objects). There are open-source tools that can be used to rehydrate tweets (for example, [Hydrator](https://github.com/DocNow/hydrator)) if researchers prefer not to write their own script to do this.

### Code
We provide a number of Python files that contain the code we used to extract data from our database, then process, analyze, and visualize that data.  

To ensure compliance with Twitter's guidelines, we cannot provide intermediate datafiles that we created as part of our extract-process-analyze-visualize pipeline. However, researchers can use the code in these files to replicate our analyses using any dataset of hydrated tweets.  
1) language_breakdown_check.py: creates the chart showing the relative proportion of different languages.
2) daily_count.py: generates  descriptive statistics and visualizations about the number of tweets per day.
3) confirm_mongo_account_info_w_id.py: generates descriptive statistics and visualizations about:
    - the number of users in the dataset
    - the number of tweets per user
    - the number of unique user descriptions per user
4) tweet_type_count.py: counts the number of retweets, quote tweets, and original tweets.
5) get_retwets_w_userid.py: generates descriptive statistics and visualizations about:
    - the number of times each tweet was retweeted
    - the number of times each account was retweeted
    - the number of times each account created retweets
    - the number of times accounts retweeted tweets originally sent by themselves
6) user_account_create_date.py: generates descriptive statistics and visualizations about when accounts that appear in our dataset were created.
7) user_bio_token_count.py: generates visualizations about the top unigrams, bigrams, and @mentions in user bios.
8) process_tweet_text.py: generates visualizations about the top unigrams and bigrams in the text of tweets.
