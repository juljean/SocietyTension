import db_connection
import matplotlib.pyplot as plt
import numpy as np


def get_sentiment_range():
    start_date = '2022-10-18'
    final_date = '2022-11-05'
    result = db_connection.connect("sentiment_range", start_date=start_date, final_date=final_date)
    print(result)
    dates = np.array([sent[0] for sent in result])
    sentiments = np.array([sent[1] for sent in result])
    print(sentiments)
    # plt.xticks(sentiments, dates)
    plt.plot(sentiments)
    plt.show()


get_sentiment_range()