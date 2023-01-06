import os
import json
import googleapiclient.discovery
import pandas as pd
import configs
import constants


def fetch_comments(fetch_parameter):
    """
    Fetch data about videos/comments from YouTube API. Uncomment the block to save data in JSON
    :param fetch_parameter: str, name of value which has to be fetched: videos/comments (branching may be continued)
    :return: json_object with fetched data
    """
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    # Address name of folder to be saved in
    json_folder = ""
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=configs.DEVELOPER_KEY)
    if fetch_parameter == "videos":
        request = youtube.search().list(
            part="snippet",
            channelId=constants.CHANNEL_ID,
            maxResults=100,
            order="viewCount"
        )
        json_folder = "ChannelVideoData/"

    if fetch_parameter == "comments":
        request = youtube.commentThreads().list(
            part="snippet",
            maxResults=100,
            videoId=constants.VIDEO_ID
        )
        json_folder = "VideoCommentData/"

    response = request.execute()["items"]
    # # To save file in json
    # json_object = json.dumps(response, indent=4, ensure_ascii=False)
    #
    # with open(json_folder + str(constants.VIDEO_ID) + ".json", "w", encoding='utf-8') as outfile:
    #     outfile.write(json_object)
    return response


def create_dataframe(json_object, folder_name, id, initial_columns, final_columns):
    df = pd.json_normalize(json_object)
    print(df)
    df.drop(columns=df.columns.difference(initial_columns), axis=1, inplace=True)
    df.set_axis(final_columns, axis=1, inplace=True)
    print(df)
    df.to_csv(folder_name + str(id) + ".csv")


def main():
    # json_object = fetch_videos(CHANNEL_ID)
    json_object = fetch_comments()
    create_dataframe(json_object, "ChannelVideoData/", constants.VIDEO_ID, constants.REMAIN_COLUMNS_VIDEO, constants.FINAL_NAMES_COLUMNS_VIDEO)


if __name__ == "__main__":
    main()
