"""
This uses the file created with the following mongoexport command:
 mongoexport -u [REDACTED] -d ConspiracyTheoriesUA_5d7a4aaf1da0444b0f499b06 -c preNov2020 -f created_at,user.screen_name,user.created_at,user.verified,user.description,user.lang,user.location,user.id --type csv --authenticationMechanism MONGODB-CR --authenticationDatabase admin
"""
import pandas as pd
import nltk
import string
import datetime
import os
import emoji
import matplotlib.pyplot as plt
from ast import literal_eval

output_folder = 'all_users/'

bio_info = pd.read_csv('preNov2020_mongo_export_user_info_w_id.csv', usecols=['user.id', 'user.description'])
bio_info.drop_duplicates(inplace=True)
bio_info.set_index('user.id', inplace=True)

tknzr = nltk.tokenize.TweetTokenizer(preserve_case=False)
stopwords = nltk.corpus.stopwords.words('english')

user_counts = bio_info.index.value_counts()
single_users = user_counts[user_counts == 1]
multi_users = user_counts[user_counts > 1]

bio_info.dropna(inplace=True)
bio_info['user.description'] = bio_info['user.description'].str.replace('#', '')
bio_info['tokens'] = bio_info['user.description'].apply(tknzr.tokenize)

grouped_bio_info = bio_info.groupby(by='user.id')
user_count = len(grouped_bio_info)

"""
To generate top unigrams and @mentions, we first create an artificial composite bio for each unique user that contains
 all of the unique tokens that appear in any that users bio. 
"""
final_user_bio = []
final_user_atmentions = []
print("{} Beginning to process users".format(datetime.datetime.now()))
user_counter = 0
for userid, user_info in grouped_bio_info:
    user_counter += 1
    user_bios = list(user_info['tokens'])
    user_tokened_bio = [token for bio in user_bios for token in bio] # This flattens nested lists of tokens
    user_tokened_bio = list(pd.unique(user_tokened_bio)) # This makes sure we only have 1 of each unique token for a user
    final_user_atmentions.extend([f for f in user_tokened_bio if f.startswith('@')])
    final_user_bio.extend(user_tokened_bio)
    final_user_bio.append('~!~ end of bio ~!~') # This is an easily identified separator between users
    if user_counter % 100000 == 0:
        print('{} {} of {} users processed for unigrams'.format(datetime.datetime.now(), user_counter, user_count))

"""
To generate top bigrams, we first create a list of all unique bigrams for each unique user, regardless of how many
 different bios that user has had. 
Bigram cleaning is built in here.
"""
final_users_bigrams = []
print("{} Beginning to process users".format(datetime.datetime.now()))
user_counter = 0
for userid, user_info in grouped_bio_info:
    user_counter += 1
    user_bios = list(user_info['tokens'])
    cleaned_user_bigrams = []
    for bio in user_bios:
        bio_bigrams = list(nltk.bigrams(bio))
        for bigram in bio_bigrams:
            keep = True
            if bigram[0] == '~!~ end of bio ~!~' or bigram[1] == '~!~ end of bio ~!~':
                keep = False
            elif bigram[0] in stopwords or bigram[1] in stopwords:
                keep = False
            elif bigram[0] in string.punctuation or bigram[1] in string.punctuation:
                keep = False
            if keep == True:
                cleaned_user_bigrams.append(bigram)
    cleaned_user_bigrams = list(pd.unique(cleaned_user_bigrams))
    final_users_bigrams.extend(cleaned_user_bigrams)
    if user_counter % 100000 == 0:
        print('{} {} of {} users processed for bigrams'.format(datetime.datetime.now(), user_counter, user_count))

print("{} User processing for unigrams complete complete!".format(datetime.datetime.now()))

bio_info = None

print("{} Cleaning unigrams".format(datetime.datetime.now()))
bio_unigrams = []
for token in final_user_bio:
    keep = True
    if token in stopwords:
        keep = False
    if token in string.punctuation:
        keep = False
    if len(token) == 1:
        if token in string.ascii_lowercase:
            keep = False
    if token == '~!~ end of bio ~!~':
        keep = False
    if keep == True:
        bio_unigrams.append(token)

print("{} Unigram cleaning complete!".format(datetime.datetime.now()))

bio_atmention_counts = nltk.FreqDist(final_user_atmentions)
bio_atmention_counts = pd.DataFrame.from_dict(dict(bio_atmention_counts), orient='index')
bio_atmention_counts.sort_values(ascending=False, by=0, inplace=True)
bio_atmention_counts['prevalence'] = bio_atmention_counts[0] / user_count
print("{} @mention counting complete".format(datetime.datetime.now()))

bio_unigram_counts = nltk.FreqDist(bio_unigrams)
bio_unigram_counts = pd.DataFrame.from_dict(dict(bio_unigram_counts), orient='index')
bio_unigram_counts.sort_values(ascending=False, by=0, inplace=True)
bio_unigram_counts['prevalence'] = bio_unigram_counts[0] / user_count
print("{} Unigram prevalence counting complete".format(datetime.datetime.now()))

bio_bigram_counts = nltk.FreqDist(final_users_bigrams)
bio_bigram_counts = pd.DataFrame.from_dict(dict(bio_bigram_counts), orient='index')
bio_bigram_counts.sort_values(ascending=False, by=0, inplace=True)
bio_bigram_counts['prevalence'] = bio_bigram_counts[0] / user_count
print("{} Bigram prevalence counting complete".format(datetime.datetime.now()))

top_subset_folder = output_folder + 'top_n/'
os.makedirs(top_subset_folder, exist_ok=True)

bio_atmention_counts.to_csv(output_folder + 'bio_at_mention_counts.csv')
bio_unigram_counts.to_csv(output_folder + 'bio_unigram_counts.csv')
bio_bigram_counts.to_csv(output_folder + 'bio_bigram_counts.csv')

top_atmentions = bio_atmention_counts.head(n=1000)
top_unigrams = bio_unigram_counts.head(n=1000)
top_bigrams = bio_bigram_counts.head(n=1000)

top_atmentions.to_csv(top_subset_folder + 'bio_at_mention_counts.csv')
top_unigrams.to_csv(top_subset_folder + 'bio_unigram_counts.csv')
top_bigrams.to_csv(top_subset_folder + 'bio_bigram_counts.csv')


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

top_bigrams = top_bigrams.head(50)
top_bigrams.loc["('🇺', '🇸')"] += top_bigrams.loc["('🇺', '🇲')"]
top_bigrams.drop(index="('🇺', '🇲')", inplace=True)

all_bigrams = list(top_bigrams.index)
all_bigrams = [literal_eval(bigram) for bigram in all_bigrams]

unigrams_to_ignore = [top_unigrams.index[0], top_unigrams.index[1], top_unigrams.index[22], '...', '🇺', '🇸', 'i\'m', 'wwg', '1wga', 'like', 'one', '•', '”', '“']
at_mention_to_ignore = ['@', '@gmail']

bigrams_to_ignore = []
for bigram in all_bigrams:
    keep = True
    if '\u200d' in bigram:
        keep = False
    elif len(bigram[0]) == 0 or len(bigram[1]) == 0:
        keep = False
    elif bigram[0] in string.punctuation or bigram[1] in string.punctuation:
        keep = False
    elif bigram[0] in string.whitespace or bigram[1] in string.whitespace:
        keep = False
    elif bigram[0].encode() == b'\xef\xb8\x8f' or bigram[1].encode() == b'\xef\xb8\x8f':
        keep = False
    elif bigram[0] in emoji.UNICODE_EMOJI or bigram[1] in emoji.UNICODE_EMOJI:
        if not (bigram[0] in emoji.UNICODE_EMOJI and bigram[1] in emoji.UNICODE_EMOJI):
            keep = False
    if keep == False:
        bigrams_to_ignore.append(str(bigram))

top_unigrams.drop(index=unigrams_to_ignore, inplace=True)
top_atmentions.drop(index=at_mention_to_ignore, inplace=True)
top_bigrams.drop(index=bigrams_to_ignore, inplace=True)

cleaned_bigram_index = top_bigrams.index.to_list()
cleaned_bigram_index = [literal_eval(bigram) for bigram in cleaned_bigram_index]
cleaned_bigram_index = [' '.join(bigram) for bigram in cleaned_bigram_index]
cleaned_bigram_index = [join_emoji(bigram) for bigram in cleaned_bigram_index]
top_bigrams.index = cleaned_bigram_index

unigram_chart = top_unigrams.head(n=30)[::-1].plot.barh(y='prevalence',  legend=None)
plt.tight_layout()
x_tick_labels = unigram_chart.get_xticks().tolist()
x_tick_labels = [str(float(label) * 100) + '%' for label in x_tick_labels]
unigram_chart.set_xticklabels(x_tick_labels)
unigram_chart.tick_params(axis='y', length=0)
for border in unigram_chart.spines:
    unigram_chart.spines[border].set_visible(False)
plt.show()
#plt.savefig('top_bio_unigrams.pdf')

at_mention_chart = top_atmentions.head(n=20)[::-1].plot.barh(y='prevalence',   legend=None)
plt.tight_layout()
for label in at_mention_chart.get_xticklabels():
    label.set_horizontalalignment('right')
x_tick_labels = at_mention_chart.get_xticks().tolist()
x_tick_labels = [str(float(label) * 100) + '%' for label in x_tick_labels]
for border in at_mention_chart.spines:
    at_mention_chart.spines[border].set_visible(False)
at_mention_chart.set_xticklabels(x_tick_labels)
at_mention_chart.tick_params(axis='y', length=0)
plt.show()
#plt.savefig('top_bio_mentions.pdf')

#top_bigrams.drop(index='maga🇺', inplace=True)
#top_bigrams.drop(index='🇸maga', inplace=True)
#top_bigrams.drop(index='🇸🇺', inplace=True)

bigram_chart = top_bigrams[::-1].plot.barh(y='prevalence', legend=None)
plt.tight_layout()
x_tick_labels = bigram_chart.get_xticks().tolist()
x_tick_labels = [str(float(label) * 100) + '%' for label in x_tick_labels]
bigram_chart.set_xticklabels(x_tick_labels)
bigram_chart.tick_params(axis='y', length=0)
for border in bigram_chart.spines:
    bigram_chart.spines[border].set_visible(False)
plt.show()
#plt.savefig('top_bio_bigrams.svg')
