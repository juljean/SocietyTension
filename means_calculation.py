import pandas as pd
import db_connection
from collections import defaultdict


def get_final_means():
    duplicates = defaultdict(list)

    result = db_connection.connect("get_date_sentiment")

    dates = [sent[0] for sent in result]
    sentiments = [sent[1] for sent in result]

    # iterate over positions and numbers simultaneously
    for i, number in enumerate(dates):
        # accumulate positions to the same number
        duplicates[number].append(i)

    duplicates_pos = {key: value for key, value in duplicates.items()}

    final_means = {}
    final_dates = []
    final_sentiments = []
    for key, value in duplicates_pos.items():
        final_means[key] = sum(sentiments[index] for index in value)/ len(value)
        final_dates.append(key)
        final_sentiments.append(sum(sentiments[index] for index in value)/len(value))

    df_final_means = pd.DataFrame(zip(final_dates, final_sentiments), columns=['observation_date', 'mean_sentiment'])
    return df_final_means
