import psycopg2
import configs
import fetch
import constants
import psycopg2.extras as extras
import data_preprocessing


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


def connect_and_insert(data):
    """
    Connect to the PostgreSQL database server
    :param data: str, data to insert: "channels"/"videos"/"comments"
    :return:
    """
    conn = None
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
        else:
            cur.execute(constants.QUERY_SELECT_VIDEO_IDS)
        # Iterative insert for dataframes referring to ids
        row = cur.fetchone()
        while row is not None:
            # row = ('NO_0s6VSBzw',), slice purifies punctuation
            id = str(row)[2: -3]

            # Insert channel data
            if data == "channels":
                df = fetch.accessAPI(id, data)
                execute_values(conn, df, 'channel_information')
            row = cur.fetchone()

            # Insert video data
            if data == "videos":
                df = fetch.accessAPI(id, data)
                execute_values(conn, df, 'video_information')

            # Insert comment data
            if data == "comments":
                df = data_preprocessing.process_comments(id, data)
                execute_values(conn, df, 'comment_information')

        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect_and_insert("channels")
