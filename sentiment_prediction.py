import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import k_means_model
from db_connection import db_communicator


def create_tfidf_dictionary(df_row, transformed_file, features):
    """
    Create dictionary for each input sentence, where each word has assigned its tfidf score
    :param df_row: pandas.Series, row of dataframe, containing sentences, and their indexes,
    :param transformed_file: all sentences transformed with TfidfVectorizer
    :param features: names of all words in corpus used in TfidfVectorizer
    :return: dict with sentence and its tfidf score
    """
    # Convert sparse matrix into coo format
    vector_coo = transformed_file[df_row.name].tocoo()
    vector_coo.col = features.iloc[vector_coo.col].values
    # Convert matrix into dictionary
    sentence_vs_tfidf_score = dict(zip(vector_coo.col, vector_coo.data))
    return sentence_vs_tfidf_score


def replace_tfidf_words(df_row, transformed_file, features):
    """
    Replacing each word with it's calculated tfidf dictionary with scores of each word
    :param df_row: row of dataframe, containing sentences, and their indexes
    :param transformed_file: all sentences transformed with TfidfVectorizer
    :param features: names of all words in corpus used in TfidfVectorizer
    :return: list with TF-IDF scores for each word
    """

    dictionary = create_tfidf_dictionary(df_row, transformed_file, features)
    return list(map(lambda y: dictionary[f'{y}'], df_row.title.split()))


def replace_sentiment_words(word, sentiment_dict):
    """
    Replacing each word with its associated sentiment score from sentiment dict
    :param word: str
    :param sentiment_dict: dict, containing this word
    :return: sentiment
    """
    try:
        out = sentiment_dict[word]
    except KeyError:
        out = 0
    return out


def set_sentiment_prediction():
    """
    Performs sentiment prediction on the comments by replacing words with TF-IDF scores and sentiment coefficients.
    :return:
    """
    # Get video IDs from the channel info using db_communicator
    video_ids = db_communicator.get_video_ids_from_channel_info()

    # Iterate over each video ID
    for video_id in video_ids:
        # Get test data for the current video ID using db_communicator
        test_ids, test_comments = db_communicator.get_test_data(video_id=str(video_id))

        # Create a DataFrame with comment IDs and titles
        file_weighting = pd.DataFrame(list(zip(test_ids, test_comments)), columns=['comment_id', 'title'])

        # Perform clusterization to get sentiment map using k_means_model
        sentiment_map = k_means_model.clusterization()

        # Create a dictionary mapping words to sentiment coefficients
        sentiment_dict = dict(zip(sentiment_map.words.values, sentiment_map.sentiment_coeff.values))

        # Create a TF-IDF vectorizer
        tfidf = TfidfVectorizer(tokenizer=lambda y: y.split(), norm=None)

        try:
            # Fit the TF-IDF vectorizer on the comment titles
            tfidf.fit(file_weighting.title)

            # Get the feature names from the TF-IDF vectorizer
            features = pd.Series(tfidf.get_feature_names_out())

            # Transform the comment titles using TF-IDF vectorization
            transformed = tfidf.transform(file_weighting.title)

            # Replace words in the titles with TF-IDF scores
            replaced_tfidf_scores = file_weighting.apply(lambda x: replace_tfidf_words(x, transformed, features), axis=1)

            # Replace words in the titles with sentiment coefficients
            replaced_closeness_scores = file_weighting.title.apply(
                lambda x: list(map(lambda y: replace_sentiment_words(y, sentiment_dict), x.split()))
            )

            # Create a DataFrame with replaced scores and original data
            replacement_df = pd.DataFrame(
                data=[replaced_closeness_scores, replaced_tfidf_scores, file_weighting.title, file_weighting.comment_id]
            ).T
            replacement_df.columns = ['sentiment_coeff', 'tfidf_scores', 'sentence', 'comment_id']

            # Calculate sentiment rate based on coefficients and scores
            replacement_df['sentiment_rate'] = replacement_df.apply(
                lambda x: np.array(x.loc['sentiment_coeff']) @ np.array(x.loc['tfidf_scores']), axis=1
            )

            # Perform sentiment prediction based on sentiment rate
            replacement_df['prediction'] = (replacement_df.sentiment_rate > 0).astype('int8')
            replacement_df['sentiment'] = [1 if i > -300 else 0 for i in replacement_df.sentiment_rate]

            # Insert sentiment values into the database using db_communicator
            for index, row in replacement_df[['comment_id', 'sentiment']].iterrows():
                db_communicator.insert_sentiment(float(row['sentiment']), str(row['comment_id']))

        except ValueError:
            # Handle the ValueError if it occurs during TF-IDF fitting
            pass


set_sentiment_prediction()