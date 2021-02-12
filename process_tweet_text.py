import pymongo
import string
import nltk
import pandas as pd
import emoji
import re
from ast import literal_eval
import datetime
import os
import sys
import matplotlib.pyplot as plt

output_file = 'all_text_by_line.txt'
os.makedirs(os.path.dirname(output_file), exist_ok=True)

mongo_details = {'address': None, # If address is None, use local mongo
                 'auth': True, # If auth is True, mongo has authentication turned on and the code needs a mongo user and pass
                 'project_name': 'ConspiracyTheoriesUA', # The name of the project for pulling data. This code will automatically use this to generate the project config DB and project data DB names.
                 'col_name': 'preNov2020', # The name of the collection containing the data to be pulled
                 'user': input('Enter the mongo username: '), 'password': input('Enter the mongo password: ') } # The user will be prompted for the mongo username and password


def build_mongo_connection():
    mongoClient = pymongo.MongoClient(mongo_details['address'])
    mongoClient.admin.authenticate(mongo_details['user'], mongo_details['password'])
    databases = mongoClient.database_names()
    project_dbs = [f for f in databases if mongo_details['project_name'] in f]
    if len(project_dbs) == 0:
        print('No databases found for the specified project. Is the project name in data_pull_config.py correct?')
        sys.exit()
    elif len(project_dbs) > 2:
        print('The specified project name returns too many results. Is the project name in data_pull_config.py correct?')
        sys.exit()
    project_config_db = [f for f in project_dbs if 'Config' in f][0]
    project_config_db = mongoClient[project_config_db]['config']
    project_data_db = [f for f in project_dbs if '_' in f][0]
    project_data_db = mongoClient[project_data_db][mongo_details['col_name']]
    return project_config_db, project_data_db

project_config_db, project_data_db = build_mongo_connection()
tweet_text_cursor = list(project_data_db.aggregate([{'$group':{'_id': '$stack_vars.full_tweet_text'}}], allowDiskUse=True))
tweet_text_cursor = pd.DataFrame(tweet_text_cursor)
tweet_text_cursor.to_csv(output_file, index=None)

"""
Need to split the text into 3 batches for the sake of RAM
"""

tweet_text_cursor.dropna(inplace=True)
total_tweet_count = len(tweet_text_cursor)
three_batches_size = round(len(tweet_text_cursor)/3, 0)
first_batch = tweet_text_cursor.loc[:three_batches_size]
first_batch.to_csv('first_batch_text_by_line.txt')
del first_batch
second_batch = tweet_text_cursor.loc[three_batches_size:2*three_batches_size]
second_batch.to_csv('second_batch_text_by_line.txt')
del second_batch
third_batch = tweet_text_cursor.loc[three_batches_size*2:]
third_batch.to_csv('third_batch_text_by_line.txt')
del third_batch
del tweet_text_cursor

tknzr = nltk.tokenize.TweetTokenizer(preserve_case=False)
stopwords = nltk.corpus.stopwords.words('english')


all_unigrams_count = pd.DataFrame()
all_emoji_count = pd.DataFrame()
all_atmention_count = pd.DataFrame()
batch_counter = 0
batch_files = ['first_batch_text_by_line.txt', 'second_batch_text_by_line.txt', 'third_batch_text_by_line.txt']
for file in batch_files:
    batch_counter += 1
    print("{} Reading batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_texts = pd.read_csv(file)
    print("{} Cleaning batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_texts['_id'] = tweet_texts['_id'].str.replace('#', '')
    tweet_texts['_id'] = tweet_texts['_id'].str.replace(re.compile('[' + string.whitespace + ']'), ' ')
    #
    print("{} Tokenizing batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_texts['tokens'] = tweet_texts['_id'].apply(tknzr.tokenize)
    #
    unique_tweet_count = len(tweet_texts)
    print("{} De-duping individual tweets in batch {}".format(datetime.datetime.now(), batch_counter))
    # The next few lines took forever to figure out. Future Sam, be proud of past Sam
    tweet_texts_deduped = tweet_texts['tokens'].apply(pd.unique)
    all_tweet_texts = list(tweet_texts_deduped)
    tweet_texts_deduped = []
    for tweet in all_tweet_texts:
        tweet = list(tweet)
        tweet.append('~!~ end of tweet ~!~')
        tweet_texts_deduped.append(tweet)
    #
    del all_tweet_texts
    print("{} Flattening token list in batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_texts_deduped = [word for tweet in tweet_texts_deduped for word in tweet]
    #
    print("{} Processing unigrams in batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_unigrams = []
    for token in tweet_texts_deduped:
        keep = True
        if token in stopwords:
            keep = False
        elif token in string.punctuation:
            keep = False
        elif len(token) == 1:
            if token in string.ascii_lowercase:
                keep = False
        elif token == '~!~ end of tweet ~!~':
            keep = False
        elif not re.search('[a-z]', token):
            keep = False
        if keep == True:
            tweet_unigrams.append(token)
    #
    del tweet_texts_deduped
    print("{} Counting ngrams in batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_unigram_counts = nltk.FreqDist(tweet_unigrams)
    tweet_unigram_counts = pd.DataFrame.from_dict(dict(tweet_unigram_counts), orient='index')
    tweet_unigram_counts.sort_values(ascending=False, by=0, inplace=True)
    tweet_unigram_counts.rename(columns={0: 'batch {}'.format(batch_counter)}, inplace=True)
    all_unigrams_count = all_unigrams_count.join(tweet_unigram_counts, how='outer')
    print("{} Batch {} unigram counting complete".format(datetime.datetime.now(), batch_counter))
    #
    tweet_texts = list(tweet_texts['tokens'])
    #
    print("{} Creating bigrams in batch {}".format(datetime.datetime.now(), batch_counter))
    tweet_text_bigrams = [list(nltk.bigrams(tweet_tokens)) for tweet_tokens in tweet_texts]
    tweet_text_bigrams = [pd.unique(tweet_bigrams) for tweet_bigrams in tweet_text_bigrams]
    print("{} Cleaning bigrams in batch {}".format(datetime.datetime.now(), batch_counter))
    batch_bigrams = []
    for bigrams in tweet_text_bigrams:
        for bigram in bigrams:
            keep = True
            if bigram[0] in stopwords or bigram[1] in stopwords:
                keep = False
            elif bigram[0] in string.punctuation or bigram[1] in string.punctuation:
                keep = False
            if keep == True:
                batch_bigrams.append(bigram)
    #
    del tweet_text_bigrams
    tweet_bigram_counts = nltk.FreqDist(batch_bigrams)
    tweet_bigram_counts = pd.DataFrame.from_dict(dict(tweet_bigram_counts), orient='index')
    tweet_bigram_counts.sort_values(ascending=False, by=0, inplace=True)
    tweet_bigram_counts.rename(columns={0: 'batch {}'.format(batch_counter)}, inplace=True)
    all_bigrams_count = all_bigrams_count.join(tweet_bigram_counts, how='outer')
    print("{} Batch {} bigram counting complete".format(datetime.datetime.now(), batch_counter))
    del tweet_bigram_counts


"""
Next step is summing across columns for ngram counts
"""

all_unigrams_count['total_count'] = all_unigrams_count.sum(1)
all_unigrams_count['prevalence'] = all_unigrams_count['total_count'] / total_tweet_count
all_unigrams_count.drop(columns=['batch 1', 'batch 2', 'batch 3'], inplace=True)
all_unigrams_count.sort_values(by='total_count', ascending=False, inplace=True)

all_bigrams_count['total_count'] = all_bigrams_count.sum(1)
all_bigrams_count['prevalence'] = all_bigrams_count['total_count'] / total_tweet_count
all_bigrams_count.drop(columns=['batch 1', 'batch 2', 'batch 3'], inplace=True)
all_bigrams_count.sort_values(by='total_count', ascending=False, inplace=True)

print("{} Writing top data to top_n_tweet_{unigram|bigram}_counts.csv".format(datetime.datetime.now()))
top_unigrams = all_unigrams_count.head(n=1000)
top_bigrams = all_bigrams_count.head(n=1000)

top_unigrams.to_csv('top_n_tweet_unigram_counts.csv')
top_bigrams.to_csv('top_n_tweet_bigram_counts.csv')

del all_unigrams_count, all_bigrams_count


##############################
#
# The next section makes viz
#
##############################


def join_emoji(bigram):
    if type(bigram) == tuple:
        bigram = ' '.join(bigram)
    emoji_present = emoji.emoji_lis(bigram)
    if len(emoji_present) > 0:
        bigram = bigram.replace(' ','')
    return bigram

unigram_links = [unigram for unigram in top_unigrams.index if '://' in unigram]
top_unigrams.drop(index=unigram_links, inplace=True)


cleaned_bigram_index = top_bigrams.index.to_list()
cleaned_bigram_index = [literal_eval(bigram) for bigram in cleaned_bigram_index]
cleaned_bigram_index = [' '.join(bigram) for bigram in cleaned_bigram_index]
cleaned_bigram_index = [join_emoji(bigram) for bigram in cleaned_bigram_index]
top_bigrams.index = cleaned_bigram_index


unigram_chart = top_unigrams.head(n=30)[::-1].plot.barh(y='prevalence',  legend=None)
plt.tight_layout()
x_tick_labels = unigram_chart.get_xticks().tolist()
x_tick_labels = [str(round(float(label) * 100, 1)) + '%' for label in x_tick_labels]
unigram_chart.set_xticklabels(x_tick_labels)
unigram_chart.tick_params(axis='y', length=0)
for border in unigram_chart.spines:
    unigram_chart.spines[border].set_visible(False)
plt.show()
#plt.savefig('top_tweet_unigrams.svg')


bigram_chart = top_bigrams.head(n=30)[::-1].plot.barh(y='prevalence', legend=None)
plt.tight_layout()
x_tick_labels = bigram_chart.get_xticks().tolist()
x_tick_labels = [str(float(label) * 100) + '%' for label in x_tick_labels]
bigram_chart.set_xticklabels(x_tick_labels)
bigram_chart.tick_params(axis='y', length=0)
for border in bigram_chart.spines:
    bigram_chart.spines[border].set_visible(False)
plt.show()
#plt.savefig('top_tweet_bigrams.svg')
