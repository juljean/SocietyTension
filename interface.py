from db_connection import db_communicator
import regex
from colorama import Fore
from datetime import datetime
import time
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import pandas as pd


def date_validator(text):
    ok = regex.match('^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$', text)
    if not ok:
        print('Будь-ласка введи дату у форматі:рррр-мм-дд')
        text = input()
        return date_validator(text)
    datetime_object = datetime.strptime(text, '%Y-%m-%d').date()
    datetime_lower_bound = datetime.strptime('2016-01-01', '%Y-%m-%d').date()
    datetime_upper_bound = datetime.now().date()
    if datetime_object < datetime_lower_bound:
        print('Ця дата надто рання. SocialDynamicsUA аналізує дані після 2016 року')
        text = input()
        return date_validator(text)
    if datetime_object > datetime_upper_bound:
        print('Твоя дата в майбутьному.Будь-ласка вводь дату включно із сьогоднішнім числом')
        text = input()
        return date_validator(text)
    return text


def get_sentiment_range(start_date, end_date):
    start_time = time.time()
    result = sorted(db_communicator.get_sentiment_range(start_date=start_date, final_date=end_date))
    dates = [str(sent[0]) for sent in result]
    # # For visual distinguishing replace 0 sentiment with 0.01
    # sentiments = [0.01 if sent[1] == 0 else sent[1] for sent in result]

    newdata = []
    for item in result:
        if item[1] != 0.0 and item[1] != 1.0:
            newdata.append(item)
    sentiments = newdata

    # Separate the dates and values into separate lists
    dates = [item[0] for item in sentiments]
    values = [item[1] for item in sentiments]

    # Convert dates to strings for plotting
    dates_str = [str(date) for date in dates]
    # Calculate average and mean values per day
    averages = []
    means = []
    for date in dates:
        avg = np.mean(values)
        mean = np.median(values)
        averages.append(avg)
        means.append(mean)

    # Reduce the number of dates shown on the x-axis
    num_dates_to_show = 20
    step_size = len(dates) // num_dates_to_show
    if step_size == 0:
        step_size = 1
    dates_str_to_show = dates_str[::step_size]

    df = pd.DataFrame(sentiments, columns=['Date', 'Value'])
    df['Date'] = pd.to_datetime(df['Date'])  # Convert dates to pandas datetime format
    df = df.set_index('Date')  # Set the Date column as the DataFrame index
    rolling_avg = df['Value'].rolling(7).mean()

    # Plotting the values and mean over selected dates
    plt.figure(figsize=(10, 6))

    for item in sentiments:
        if item[1] > averages[0]:
            plt.plot(str(item[0]), item[1], color='#FEB9C6', marker='o', linestyle=' ')
        else:
            plt.plot(str(item[0]), item[1], color='#B96B85', marker='o', linestyle=' ')

    plt.plot(dates_str, rolling_avg, color='#021E20', label='Rolling Average', linewidth=1.5)
    plt.plot(dates_str, means, color='#048399', label='Mean', linewidth=2.5)
    plt.plot(dates_str, averages, color='#005067', label='Average', linewidth=2.5)
    plt.xlabel('Дата')
    plt.ylabel('Напруженість суспільства')
    plt.title(f'Графік напруженості супільства у часовому проміжку від {start_date} до {end_date}')

    plt.xticks(dates_str_to_show, fontsize=8, rotation=90)
    plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1], fontsize=8)

    plt.legend()
    plt.grid(True)

    print(f"Швидкість обробки запиту = {time.time() - start_time}")
    plt.show()


def main():
    print(
        Fore.BLUE + 'Привіт! Ти користуєшся SocialDynamicsUA. \n Введи дату у форматі рррр-мм-дд для визначення '
                    'напруженості суспільства у введеному часовому проміжку')
    # Date type format '2022-02-23'
    start_date = date_validator(input(Fore.WHITE + "Введи початок часового проміжку:"))
    end_date = date_validator(input("Введи кінець часового проміжку:"))
    get_sentiment_range(start_date, end_date)

