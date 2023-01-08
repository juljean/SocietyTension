import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import db_connection


def create_tfidf_dictionary(x, transformed_file, features):
    '''
    create dictionary for each input sentence x, where each word has assigned its tfidf score

    inspired  by function from this wonderful article:
    https://medium.com/analytics-vidhya/automated-keyword-extraction-from-articles-using-nlp-bfd864f41b34

    x - row of dataframe, containing sentences, and their indexes,
    transformed_file - all sentences transformed with TfidfVectorizer
    features - names of all words in corpus used in TfidfVectorizer

    '''
    vector_coo = transformed_file[x.name].tocoo()
    vector_coo.col = features.iloc[vector_coo.col].values
    dict_from_coo = dict(zip(vector_coo.col, vector_coo.data))
    return dict_from_coo


def replace_tfidf_words(x, transformed_file, features):
    '''
    replacing each word with it's calculated tfidf dictionary with scores of each word
    x - row of dataframe, containing sentences, and their indexes,
    transformed_file - all sentences transformed with TfidfVectorizer
    features - names of all words in corpus used in TfidfVectorizer
    '''
    dictionary = create_tfidf_dictionary(x, transformed_file, features)
    return list(map(lambda y: dictionary[f'{y}'], x.title.split()))


def replace_sentiment_words(word, sentiment_dict):
    '''
    replacing each word with its associated sentiment score from sentiment dict
    '''
    try:
        out = sentiment_dict[word]
    except KeyError:
        out = 0
    return out

video_ids = db_connection.connect('videos')

for video_id in video_ids:
    print(video_id, str(video_id)[2: -3])
    # List of [comment_id, comment] format is a test data format
    test_data = db_connection.connect("get_test_data", video_id=str(video_id)[2: -3])
    print(test_data)
    text_id = [sent[0] for sent in test_data]
    text_data = [sent[1].lower() for sent in test_data]
    file_weighting = pd.DataFrame(list(zip(text_id, text_data)), columns=['comment_id', "title"])

    sentiment_map = pd.read_csv('sentiment_results/sentiment_dictionary.csv')
    sentiment_dict = dict(zip(sentiment_map.words.values, sentiment_map.sentiment_coeff.values))
    tfidf = TfidfVectorizer(tokenizer=lambda y: y.split(), norm=None)
    tfidf.fit(file_weighting.title)
    features = pd.Series(tfidf.get_feature_names_out())
    transformed = tfidf.transform(file_weighting.title)

    replaced_tfidf_scores = file_weighting.apply(lambda x: replace_tfidf_words(x, transformed, features), axis=1)
    replaced_closeness_scores = file_weighting.title.apply(
        lambda x: list(map(lambda y: replace_sentiment_words(y, sentiment_dict), x.split())))

    replacement_df = pd.DataFrame(data=[replaced_closeness_scores, replaced_tfidf_scores, file_weighting.title, file_weighting.comment_id]).T
    replacement_df.columns = ['sentiment_coeff', 'tfidf_scores', 'sentence', 'comment_id']
    replacement_df['sentiment_rate'] = replacement_df.apply(
        lambda x: np.array(x.loc['sentiment_coeff']) @ np.array(x.loc['tfidf_scores']), axis=1)

    replacement_df['prediction'] = (replacement_df.sentiment_rate > 0).astype('int8')
    replacement_df['sentiment'] = [1 if i > -300 else 0 for i in replacement_df.sentiment_rate]
    replacement_df[['comment_id', 'sentence', 'sentiment']].to_csv('sentiment_results/results.csv', index=False)
    for index, row in replacement_df[['comment_id', 'sentiment']].iterrows():
        db_connection.connect('sentiment', float(row['sentiment']), str(row['comment_id']))
    # Normalization of sentiment from -1 to 1
    # replacement_df['sentiment_rate'] = round(replacement_df['sentiment_rate'] / replacement_df['sentiment_rate'].abs().max(), 4)


    predicted_classes = replacement_df.prediction
    y_test = replacement_df.sentiment
