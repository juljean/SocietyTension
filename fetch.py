import os
import json
import googleapiclient.discovery
import pandas as pd
import configs
import constants


def fetch_data(fetch_parameter, json_folder, video_id=None):
    """
    Fetch data about videos/comments from YouTube API. Uncomment the block to save data in JSON
    :param video_id: info request for each Video and Comment statistics from video_id
    :param json_folder: address name of folder to be saved in
    :param fetch_parameter: str, name of value which has to be fetched: channels/videos/comments
    :return: json_object with fetched data
    """
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=configs.DEVELOPER_KEY)
    if fetch_parameter == "channels":
        request = youtube.search().list(
            part="snippet",
            channelId=constants.CHANNEL_ID,
            maxResults=100,
            order="viewCount"
        )

    if fetch_parameter == "videos":
        request = youtube.videos().list(
            part="statistics",
            id=video_id
        )

    if fetch_parameter == "comments":
        request = youtube.commentThreads().list(
            part="snippet",
            maxResults=100,
            videoId=video_id
        )

    response = request.execute()["items"]
    # # To save file in json
    # json_object = json.dumps(response, indent=4, ensure_ascii=False)
    #
    # with open(json_folder + constants.CHANNEL_ID + "_" + str(video_id) + ".json", "w", encoding='utf-8') as outfile:
    #     outfile.write(json_object)
    return response


def create_dataframe(json_object, folder_name, parameter_id, initial_columns, final_columns):
    """
    Drop useless data, rename columns, save up the file
    :param json_object: response from YouTube API
    :param folder_name: name of the folder to save the file up
    :param parameter_id: id of the channel/video which will be the name of the saved file
    :param initial_columns: initial columns' name to keep in dataframe
    :param final_columns: renamed columns, kept in the dataframe
    :return: dataframe
    """
    df = pd.json_normalize(json_object)
    df.drop(columns=df.columns.difference(initial_columns), axis=1, inplace=True)
    df.set_axis(final_columns, axis=1, inplace=True)
    # df.to_csv(folder_name + str(parameter_id) + ".csv")
    return df


def accessAPI(video_id=constants.VIDEO_ID, fetch_parameter="comments"):
    """
    Operate the request for fetching. So far only video and comments fetching is available
    :return: df
    """
    # Fetch Channel information
    if fetch_parameter == "channels":
        json_object = fetch_data(fetch_parameter, constants.FOLDER_NAME_CHANNEL, constants.CHANNEL_ID)
        df = create_dataframe(json_object, constants.FOLDER_NAME_CHANNEL, constants.CHANNEL_ID,
                              constants.REMAIN_COLUMNS_CHANNEL, constants.FINAL_NAMES_COLUMNS_CHANNEL)
    # Fetch Video information
    if fetch_parameter == "videos":
        json_object = fetch_data(fetch_parameter, constants.FOLDER_NAME_VIDEO, video_id)
        df = create_dataframe(json_object, constants.FOLDER_NAME_VIDEO, video_id,
                              constants.REMAIN_COLUMNS_VIDEO, constants.FINAL_NAMES_COLUMNS_VIDEO)

    # Fetch Comments information
    if fetch_parameter == "comments":
        json_object = fetch_data(fetch_parameter, constants.FOLDER_NAME_COMMENT, video_id)
        df = create_dataframe(json_object, constants.FOLDER_NAME_COMMENT, video_id,
                              constants.REMAIN_COLUMNS_COMMENT, constants.FINAL_NAMES_COLUMNS_COMMENT)
    # print(df)
    return df


if __name__ == "__main__":
    accessAPI()
