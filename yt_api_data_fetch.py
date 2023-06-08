import os
import googleapiclient.discovery
import pandas as pd
import configs
import constants
import logging


def fetch_data_from_yt_api(data_fetch_parameter, channel_id=constants.CHANNEL_ID, video_id=constants.VIDEO_ID):
    """
    Fetch data about videos/comments from YouTube API. Uncomment the block to save data in JSON
    :param channel_id: str, parameter to request stats about channel
    :param video_id: str, parameter to request stats about videos or comment
    :param data_fetch_parameter: str, name of value which has to be fetched: channels/videos/comments
    :return: json_object with fetched data
    """
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=configs.DEVELOPER_KEY)
    if data_fetch_parameter == "channels":
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=100,
            order="date"
        )

    elif data_fetch_parameter == "videos":
        request = youtube.videos().list(
            part="statistics",
            id=video_id
        )

    elif data_fetch_parameter == "comments":
        request = youtube.commentThreads().list(
            part="snippet",
            maxResults=100,
            videoId=video_id
        )

    else:
        logging.error(f'data_fetch_parameter {data_fetch_parameter} is not among possible labels: '
                      f'channels/videos/comments')
        return None

    # YouTube has some limitations for data that application is using, like hidden comments
    try:
        response = request.execute()["items"]
        return response
    except Exception as err:
        logging.error(f"YouTube API faced next exception while fetching {data_fetch_parameter}: {err}")
        return None


def create_dataframe_from_yt_json(json_object, yt_api_column_names, system_column_names):
    """
    Drop useless data which YouTube API outputs, rename columns
    :param json_object: response from YouTube API
    :param yt_api_column_names: initial columns' name to keep in dataframe
    :param system_column_names: renamed columns, kept in the dataframe
    :return: dataframe with simplified YT response
    """
    df = pd.json_normalize(json_object)
    df.drop(columns=df.columns.difference(yt_api_column_names), axis=1, inplace=True)
    df.set_axis(system_column_names, axis=1, inplace=True)
    return df


def accessAPI(data_fetch_parameter="comments", video_id=constants.VIDEO_ID, channel_id=constants.CHANNEL_ID):
    """
    Operate the request for fetching. So far only video and comments fetching is available
    :param channel_id: str, parameter to request stats about channel
    :param video_id: str, parameter to request stats about videos or comment
    :param data_fetch_parameter: str, name of value which has to be fetched: channels/videos/comments

    :return: df
    """

    df = None
    # Fetch Channel information
    if data_fetch_parameter == "channels":
        json_object = fetch_data_from_yt_api(data_fetch_parameter=data_fetch_parameter, channel_id=channel_id)
        if json_object is not None:
            df = create_dataframe_from_yt_json(json_object, constants.REMAIN_COLUMNS_CHANNEL,
                                               constants.FINAL_NAMES_COLUMNS_CHANNEL)
    # Fetch Video information
    elif data_fetch_parameter == "videos":
        json_object = fetch_data_from_yt_api(data_fetch_parameter=data_fetch_parameter, video_id=video_id)
        if json_object is not None:
            df = create_dataframe_from_yt_json(json_object, constants.REMAIN_COLUMNS_VIDEO,
                                               constants.FINAL_NAMES_COLUMNS_VIDEO)

    # Fetch Comment information
    elif data_fetch_parameter == "comments":
        json_object = fetch_data_from_yt_api(data_fetch_parameter=data_fetch_parameter, video_id=video_id)
        if json_object is not None:
            df = create_dataframe_from_yt_json(json_object, constants.REMAIN_COLUMNS_COMMENT,
                                               constants.FINAL_NAMES_COLUMNS_COMMENT)
            # Custom field with language of comment. Default 'no' meaning No Language is detected
            df["original_language"] = 'no'

    else:
        logging.error(f'data_fetch_parameter {data_fetch_parameter} is not among possible labels: '
                      f'channels/videos/comments')
        return None

    return df


if __name__ == "__main__":
    accessAPI()


