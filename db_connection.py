import psycopg2

import constants


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost",
            database="SocialTension",
            user="postgres",
            password="uX4MPdBBnpyPgN")

        # create a cursor
        cur = conn.cursor()

        # cur.execute(constants.QUERY_INSERT_CHANNEL_NAME, ('UC5HBd4l_kpba5b0O1pK-Bfg', 'STERNENKO'))
        # print("query executed")

        cur.execute(constants.QUERY_SELECT_CHANNEL_NAME)
        row = cur.fetchone()

        while row is not None:
            print(row)
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


if __name__ == '__main__':
    connect()
