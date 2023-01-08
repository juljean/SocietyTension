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


def insert_data_into_table(id, data, conn):
    # row = ('NO_0s6VSBzw',), slice purifies punctuation
    # Insert channel data
    if data == "channels":
        df = fetch.accessAPI(id, data)
        execute_values(conn, df, 'channel_information')

    # Insert video data
    if data == "videos":
        df = fetch.accessAPI(id, data)
        execute_values(conn, df, 'video_information')

    # Insert comment data
    if data == "comments":
        df = data_preprocessing.process_comments(id, data)
        execute_values(conn, df, 'comment_information')


def connect(data, sentiment_value=None, comment_id=None, video_id=None, start_date=None, final_date=None):
    """
    Connect to the PostgreSQL database server
    :param final_date: datetime, for user to input range
    :param start_date: datetime, for user to input range
    :param video_id: string, parameter to fetch and process comments by multiple video ids
    :param comment_id: string of comment_id, parameter for ready sentiment insertion
    :param sentiment_value: string of sentiment value, parameter for ready sentiment insertion
    :param data: str, data to insert: "channels"/"videos"/"comments"
    :return: list with comments/None
    """
    conn = None
    # Flag when insertion into table is needed
    insert_flag = 1
    # Resulting array for selected info
    result = []
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost",
            database="SocialTension",
            user="postgres",
            password=configs.SOCIAL_TENSION_DB_PASSWORD)

        # create a cursor
        cur = conn.cursor()
        if data == "channels":
            cur.execute(constants.QUERY_SELECT_CHANNEL_IDS)
        elif data == "videos":
            insert_flag = 0
            cur.execute(constants.QUERY_SELECT_VIDEO_IDS)
        elif data == "get_train_data":
            insert_flag = 0
            cur.execute(constants.QUERY_SELECT_COMMENTS_TRAIN)
        elif data == "get_test_data":
            insert_flag = 0
            cur.execute(constants.QUERY_SELECT_COMMENTS_TEST, (video_id,))
        # To insert dataframe with mean_sentiments
        elif data == "mean_sentiment":
            df = means_calculation.get_final_means()
            execute_values(conn, df, 'sentiment_date')
            return 0
        # To fetch sentiment and published date of comment
        elif data == "get_date_sentiment":
            insert_flag = 0
            cur.execute(constants.QUERY_SELECT_DATE_SENTIMENT)
        elif data == "sentiment_range":
            insert_flag = 0
            cur.execute(constants.QUERY_SELECT_MEAN_SENTIMENT, (start_date, final_date,))
        elif data == "sentiment":
            cur.execute(constants.QUERY_INSERT_COMMENT_SENTIMENT, (sentiment_value, comment_id))
            conn.commit()
            count = cur.rowcount
            print(count, "Record Updated successfully ")
        else:
            cur.execute(constants.QUERY_SELECT_VIDEO_IDS)
        # Iterative insert for dataframes referring to ids
        row = cur.fetchone()
        while row is not None:
            new_row = str(row)[2: -3]
            if insert_flag:
                insert_data_into_table(new_row, data, conn)
            else:
                if len(row) == 2:
                    result.append([row[0], row[1]])
                else:
                    result.append(row)
            row = cur.fetchone()
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return result


if __name__ == '__main__':
    connect("mean_sentiment")
