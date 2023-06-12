import psycopg2
import configs
import yt_api_data_fetch
import constants
import psycopg2.extras as extras
import data_preprocessing
import logging


def execute_values(conn, sql_table_as_df, db_table_name):
    """
    Custom data insertion from pandas dataframe into DB table
    :param conn: PostgreSQL connection object
    :param sql_table_as_df: pandas dataframe pointer. DF has same names and order of column names as SQL table
    :param db_table_name: str, DataBase table-name to insert dataframe into
    :return:
    """
    # Method to_numpy() principle
    # Before
    # data = {'Name': ['John', 'Emma', 'Mike'],
    #         'Age': [25, 28, 30],
    #         'City': ['New York', 'Paris', 'London']}
    # After
    # ['John' 25 'New York']
    # ['Emma' 28 'Paris']
    # ['Mike' 30 'London']
    db_row_like_tuples = [tuple(x) for x in sql_table_as_df.to_numpy()]
    db_columns = ','.join(list(sql_table_as_df.columns))

    # SQL query to execute
    query = constants.QUERY_INSERT_DF % (db_table_name, db_columns)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, db_row_like_tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"While inserting dataframe into DB-table {error} has occured")
        conn.rollback()
        cursor.close()
        return 1
    logging.info("Dataframe is inserted into DB-table")
    cursor.close()


def insert_data_into_table(sample_id, table_type, conn, existing_ids):
    """

    :param sample_id: value of the primary key of the table-row to insert
    :param table_type: str, type of data, being inserted (channel info/video info/comment info)
    :param conn: PostgreSQL connection object
    :param existing_ids: list of existing ids to avoid duplication
    :return:
    """
    # row = ('NO_0s6VSBzw',), slice purifies punctuation
    # Insert channel data
    if table_type == "channels":
        # pass channel id and fetch video information
        df = yt_api_data_fetch.accessAPI(channel_id=sample_id, data_fetch_parameter=table_type)
        # TODO make logging
        if df is not None:
            df = df[~df['video_id'].isin(existing_ids)]
            if len(df) > 0:
                execute_values(conn, df, 'channel_information')

    # Insert video data
    if table_type == "videos":
        df = yt_api_data_fetch.accessAPI(video_id=sample_id, data_fetch_parameter=table_type)
        if df is not None:
            df = df[~df['video_id'].isin(existing_ids)]
            if len(df) > 0:
                execute_values(conn, df, 'video_information')

    # Insert comment data
    if table_type == "comments":
        if id not in existing_ids:
            print(f"{sample_id} is not among existing_ids")
            df = data_preprocessing.process_comments(video_id=sample_id, fetch_parameter=table_type)
            if df is not None:
                execute_values(conn, df, 'comment_information')


class DBOperations:
    def __init__(self):
        self.cur, self.conn = connect()

    @staticmethod
    def sql_output_formatting(sql_token):
        return str(sql_token)[2:-3]

    def insert_channel_info(self):
        """
        AKA insert information about videos on this channel
        :return:
        """
        self.cur.execute(constants.QUERY_SELECT_ONE_COLUMN_VALUES %
                         {"column_name": "channel_id", "table_name": "channel_name"})
        self.iterate_data_to_insert("channels")

    def insert_new_comments(self):
        """
        Insert freshly-fetched comments
        :return:
        """
        self.cur.execute(constants.QUERY_SELECT_ONE_COLUMN_VALUES %
                         {"column_name": "video_id", "table_name": "channel_information"})
        self.iterate_data_to_insert("comments")

    def get_video_ids_from_table(self, source_table):
        """
        Get existing video ids from table
        :return: list of ids
        """
        if source_table == 'channels':
            self.cur.execute(constants.QUERY_SELECT_ONE_COLUMN_VALUES %
                             {"column_name": "video_id", "table_name": "channel_information"})
        elif source_table == 'comments':
            self.cur.execute(constants.QUERY_SELECT_ONE_COLUMN_VALUES %
                             {"column_name": "video_id", "table_name": "comment_information"})
        query_result = convert_from_sql_output(self.cur)
        formatted_query_result = [self.sql_output_formatting(video_id) for video_id in query_result]
        return formatted_query_result

    def delete_late_comments(self):
        """
        Function for cleansing data and deleting comments which published date is more than *delta*
        later than video published date
        :return:
        """
        video_ids_list = self.get_video_ids_from_table('channels')
        for video_id in video_ids_list:
            self.cur.execute(constants.QUERY_DELETE_LATE_COMMENTS, (video_id, video_id))
        self.conn.commit()
        logging.info("Table is updated successfully")

    def get_comment_ids_from_comment_info(self):
        """
        Get comment ids from comment info table
        :return: list with str of comment ids
        """
        self.cur.execute(constants.QUERY_SELECT_ONE_COLUMN_VALUES %
                         {"column_name": "comment_id", "table_name": "comment_information"})
        query_result = convert_from_sql_output(self.cur)
        formatted_query_result = [self.sql_output_formatting(video_id) for video_id in query_result]
        return formatted_query_result

    def get_train_data(self):
        """
        Get all comments in *ukrainian* to train the model
        :return: list of strings representing comments
        """
        self.cur.execute(constants.QUERY_SELECT_COMMENTS_TRAIN)
        query_result = convert_from_sql_output(self.cur)
        return query_result

    def get_test_data(self, video_id):
        """
        get comments to test model
        :param video_id: string, parameter to fetch and process comments by multiple video ids
        :return: list of comments
        """
        self.cur.execute(constants.QUERY_SELECT_COMMENTS_TEST, (video_id,))
        query_result = convert_from_sql_output(self.cur)
        ids = [row[0] for row in query_result]
        comments = [row[1].lower() for row in query_result]
        return ids, comments

    def insert_mean_sentiment(self, df):
        # To insert dataframe with mean_sentiments
        execute_values(self.conn, df, 'sentiment_date')
        return 0

    def get_date_sentiment(self):
        # To fetch sentiment and published date of comment
        self.cur.execute(constants.QUERY_SELECT_DATE_SENTIMENT)
        result = convert_from_sql_output(self.cur)
        return result

    def get_sentiment_range(self, start_date, final_date):
        """

        :param final_date: datetime, for user to input range
        :param start_date: datetime, for user to input range
        :return:
        """
        self.cur.execute(constants.QUERY_SELECT_MEAN_SENTIMENT, (start_date, final_date,))
        result = convert_from_sql_output(self.cur)
        return result

    def insert_sentiment(self, sentiment_value, comment_id):
        """

        :param comment_id: string of comment_id, parameter for ready sentiment insertion
        :param sentiment_value: string of sentiment value, parameter for ready sentiment insertion
        :return:
        """
        self.cur.execute(constants.QUERY_INSERT_COMMENT_SENTIMENT, (sentiment_value, comment_id))
        self.conn.commit()
        count = self.cur.rowcount
        print(count, "Record Updated successfully ")

    def update_language(self):
        comments = self.get_train_data()
        language_data = data_preprocessing.language_recognition(comments)
        for comment_id, langauge_value in language_data.items():
            self.cur.execute(constants.QUERY_UPDATE_LANGUAGE_VALUE, (langauge_value, comment_id))
            self.conn.commit()
        print("Record Updated successfully ")

    def lemmatize_text(self):
        comments_list = self.get_train_data()
        for comment in comments_list:
            processed_comment = data_preprocessing.lemmatize_text(comment[1])
            self.cur.execute(constants.QUERY_TEXT_VALUE, (processed_comment, comment[0]))
            self.conn.commit()
        print("Record Updated successfully ")

    def iterate_data_to_insert(self, data):
        """
        Inserts data about base information: channels/videos/comments
        :param cur:
        :param data: name of data to insert(referred in insert_data_into_table())
        :param conn:
        :return:
        """
        # insertion_limit = 10
        # Iterative insert for dataframes referring to ids
        row = self.cur.fetchone()
        if data == "channels":
            # Create the list of already existing videos in channel_information table to avoid duplicates
            existing_ids = self.get_video_ids_from_table('channels')
        if data == "comments":
            existing_ids = self.get_video_ids_from_table('comments')
        while row is not None:
            new_row = str(row)[2: -3]
            insert_data_into_table(new_row, data, self.conn, existing_ids)
            row = self.cur.fetchone()
            # insertion_limit -= 1
        self.conn.commit()


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


db_communicator = DBOperations()
# db_communicator.insert_channel_info()
# db_communicator.insert_new_comments()

