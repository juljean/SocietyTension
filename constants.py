QUERY_INSERT_CHANNEL_NAME = """
INSERT INTO channel_name (channel_id, channel_name)
VALUES (%s, %s);
"""

QUERY_SELECT_CHANNEL_NAME = """
SELECT * FROM channel_name
"""