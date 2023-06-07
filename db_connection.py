import psycopg2
import configs
import fetch
import constants
import psycopg2.extras as extras
import data_preprocessing
import means_calculation


def execute_values(conn, df, table):
    """
    Custom data insertion from dataframe
    :param conn: PostgreSQL connection object
    :param df: pandas dataframe pointer. DF has same names and order of column names as SQL table
    :param table: str, SQL table name
    :return:
    """
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Dataframe is inserted")
    cursor.close()


def insert_data_into_table(id, data, conn, existing_ids):
    # row = ('NO_0s6VSBzw',), slice purifies punctuation
    # Insert channel data
    if data == "channels":
        # pass channel id and fetch video information
        df = fetch.accessAPI(channel_id=id, fetch_parameter=data)
        # TODO make logging
        if df is not None:
            df = df[~df['video_id'].isin(existing_ids)]
            if len(df) > 0:
                execute_values(conn, df, 'channel_information')

    # Insert video data
    if data == "videos":
        df = fetch.accessAPI(id, data)
        if df is not None:
            df = df[~df['video_id'].isin(existing_ids)]
            if len(df) > 0:
                execute_values(conn, df, 'video_information')

    # Insert comment data
    if data == "comments":
        if id not in existing_ids:
            print(f"{id} is not among existing_ids")
            df = data_preprocessing.process_comments(video_id=id, fetch_parameter=data)
            if df is not None:
                execute_values(conn, df, 'comment_information')


def insert_channel_info():
    """
    AKA insert information about videos on this channel
    :return:
    """
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_CHANNEL_IDS)
    iterate_data_to_insert(cur, "channels", conn)


def insert_new_comments():
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_VIDEO_IDS_FROM_CHANNEL)
    iterate_data_to_insert(cur, "comments", conn)


def get_video_ids_from_channel_info():
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_VIDEO_IDS_FROM_CHANNEL)
    result = convert_from_sql_output(cur)
    result = [str(video_id)[2: -3] for video_id in result]
    return result


# TODO fucntion below duplicates get_video_ids_from_channel_info except the query parameter. Generalize
def get_video_ids_from_comment_info():
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_VIDEO_IDS_FROM_COMMENT)
    result = convert_from_sql_output(cur)
    result = [str(video_id)[2: -3] for video_id in result]
    return result


def get_comment_ids_from_comment_info():
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_COMMENT_IDS_FROM_COMMENT)
    result = convert_from_sql_output(cur)
    result = [str(video_id)[2: -3] for video_id in result]
    return result


def get_train_data():
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_COMMENTS_TRAIN)
    result = convert_from_sql_output(cur)
    return result


def get_test_data(video_id):
    """

    :param video_id: string, parameter to fetch and process comments by multiple video ids
    :return:
    """
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_COMMENTS_TEST, (video_id,))
    result = convert_from_sql_output(cur)
    return result


def insert_mean_sentiment():
    cur, conn = connect()
    # To insert dataframe with mean_sentiments
    df = means_calculation.get_final_means()
    execute_values(conn, df, 'sentiment_date')
    return 0


def get_date_sentiment():
    # To fetch sentiment and published date of comment
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_DATE_SENTIMENT)
    result = convert_from_sql_output(cur)
    return result


def get_sentiment_range(start_date, final_date):
    """

    :param final_date: datetime, for user to input range
    :param start_date: datetime, for user to input range
    :return:
    """
    cur, conn = connect()
    cur.execute(constants.QUERY_SELECT_MEAN_SENTIMENT, (start_date, final_date,))
    result = convert_from_sql_output(cur)
    return result


def insert_sentiment(sentiment_value, comment_id):
    """

    :param comment_id: string of comment_id, parameter for ready sentiment insertion
    :param sentiment_value: string of sentiment value, parameter for ready sentiment insertion
    :return:
    """
    cur, conn = connect()
    cur.execute(constants.QUERY_INSERT_COMMENT_SENTIMENT, (sentiment_value, comment_id))
    conn.commit()
    count = cur.rowcount
    # TODO check if I really need to close it all the time. Create class??
    cur.close()
    print(count, "Record Updated successfully ")


def update_language():
    cur, conn = connect()
    comments = get_train_data()
    language_data = data_preprocessing.language_recognition(comments)
    for comment_id, langauge_value in language_data.items():
        cur.execute(constants.QUERY_UPDATE_LANGUAGE_VALUE, (langauge_value, comment_id))
        conn.commit()
        cur.close()
    print("Record Updated successfully ")


def delete_late_comments():
    cur, conn = connect()
    video_ids_list = get_video_ids_from_channel_info()
    for video_id in video_ids_list:
        cur.execute(constants.QUERY_DELETE_LATE_COMMENTS, (video_id, video_id))
    conn.commit()
    cur.close()
    print("Record Updated successfully ")


def lemmatize_text():
    cur, conn = connect()
    comments_list = get_train_data()
    for comment in comments_list:
        processed_comment = data_preprocessing.lemmatize_text(comment[1])
        cur.execute(constants.QUERY_TEXT_VALUE, (processed_comment, comment[0]))
        conn.commit()
    cur.close()
    print("Record Updated successfully ")


def convert_from_sql_output(cur):
    result = []
    row = cur.fetchone()
    while row is not None:
        if len(row) == 2:
            result.append([row[0], row[1]])
        else:
            result.append(row)
        row = cur.fetchone()
    cur.close()
    return result


def iterate_data_to_insert(cur, data, conn):
    """
    Inserts data about base information: channels/videos/comments
    :param cur:
    :param data: name of data to insert(referred in insert_data_into_table())
    :param conn:
    :return:
    """
    # insertion_limit = 10
    # Iterative insert for dataframes referring to ids
    row = cur.fetchone()
    if data == "channels":
        # Create the list of already existing videos in channel_information table to avoid duplicates
        existing_ids = get_video_ids_from_channel_info()
    if data == "comments":
        existing_ids = get_video_ids_from_comment_info()
    while row is not None:
        new_row = str(row)[2: -3]
        insert_data_into_table(new_row, data, conn, existing_ids)
        row = cur.fetchone()
        # insertion_limit -= 1
    conn.commit()
    # close the communication with the PostgreSQL
    cur.close()


def connect():
    """
    Connect to the PostgreSQL database server
    :return: cursor and conn
    """
    # Flag when insertion into table is needed
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            f'postgres://avnadmin:{configs.SOCIAL_TENSION_DB_PASSWORD}@kpi-projects-social-tension.aivencloud.com'
            f':26569/society-tension?sslmode=require')

        # create a cursor
        cur = conn.cursor()
        return cur, conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    # insert_channel_info()
    # insert_new_comments()
    insert_mean_sentiment()