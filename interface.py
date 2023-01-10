import db_connection
import matplotlib.pyplot as plt
import numpy as np
import regex
from colorama import Fore
from datetime import datetime
import time
def date_validator(text):
    ok = regex.match('^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$', text)
    if not ok:
        print('Please enter a valid date in a formate:yyyy-mm-dd')
        text = input()
        return date_validator(text)
    datetime_object = datetime.strptime(text, '%Y-%m-%d').date()
    datetime_lower_bound = datetime.strptime('2016-01-01', '%Y-%m-%d').date()
    datetime_upper_bound = datetime.now().date()
    if datetime_object < datetime_lower_bound:
        print('Your date is too early.Please enter the date after 2016')
        text = input()
        return date_validator(text)
    if datetime_object > datetime_upper_bound:
        print('Your date is in the future.Please enter the past date')
        text = input()
        return date_validator(text)
    return text


def get_sentiment_range(start_date, end_date):
    start_time = time.time()
    result = sorted(db_connection.connect("sentiment_range", start_date=start_date, final_date=end_date))
    dates = [str(sent[0]) for sent in result]
    # For visual distinguishing replace 0 sentiment with 0.01
    sentiments = [0.01 if sent[1] == 0 else sent[1] for sent in result]
    plt.xticks(ticks=np.arange(0, len(dates)), labels=dates, fontsize=8, rotation=90)
    plt.yticks(fontsize=8)
    plt.title(f'Mean sentiment from {start_date} till {end_date}')
    plt.bar(dates, sentiments, color=(0.2, 0.4, 0.6, 0.6))
    print(f"Speed of request processing = {time.time() - start_time}")
    plt.show()


def main():
    print(
        Fore.BLUE + 'Hi, welcome to SentimentUA. \nLet\'s figure out the sentiment of Ukrainian users in some data range! ')
    # Date type format '2022-02-23'
    start_date = date_validator(input(Fore.WHITE + "Input the start date of the analysis range:"))
    end_date = date_validator(input("Input the end date of the analysis range:"))
    get_sentiment_range(start_date, end_date)


main()
