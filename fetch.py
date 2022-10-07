import os
import json
import googleapiclient.discovery
import pandas as pd
import configs

#Ragulivna
CHANNEL_ID = "UCf4A8MGpasfQTa28WncfreQ"
VIDEO_ID = "ycV6bF_er3c"

REMAIN_COLUMNS_CHANNEL = ["snippet.channelId", "id.videoId", "snippet.title", "snippet.publishTime"]

FINAL_NAMES_COLUMNS_CHANNEL = ["channelId", "videoId", "title", "publishTime"]

REMAIN_COLUMNS_VIDEO = ["snippet.topLevelComment.id", "snippet.topLevelComment.snippet.videoId",
                  "snippet.topLevelComment.snippet.textOriginal",
                  "snippet.topLevelComment.snippet.authorDisplayName",
                  "snippet.topLevelComment.snippet.viewerRating,snippet.topLevelComment.snippet.likeCount",
                  "snippet.topLevelComment.snippet.publishedAt"]
FINAL_NAMES_COLUMNS_VIDEO = ['videoId','textOriginal', 'authorDisplayName', "likeCount", "publishedAt"]


def fetch_videos():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = configs.DEVELOPER_KEY

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    request = youtube.search().list(
        part="snippet",
        channelId=CHANNEL_ID,
        maxResults=100,
        order="viewCount"
    )
    response = request.execute()
    response = request.execute()["items"]
    return response

    # json_object = json.dumps(response, indent=4, ensure_ascii=False)

    # with open("ChannelVideoData/" + str(CHANNEL_ID) + ".json", "w", encoding='utf-8') as outfile:
    #     outfile.write(json_object)


def fetch_comments():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=configs.DEVELOPER_KEY)

    request = youtube.commentThreads().list(
        part="snippet",
        maxResults=100,
        videoId=VIDEO_ID
    )
    response = request.execute()["items"]

    # To save file in json
    # json_object = json.dumps(response, indent=4, ensure_ascii=False)
    #
    # with open("VideoCommentData/" + str(VIDEO_ID) + ".json", "w", encoding='utf-8') as outfile:
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
    json_object = fetch_videos()
    # json_object = fetch_comments()
    create_dataframe(json_object, "ChannelVideoData/", CHANNEL_ID, REMAIN_COLUMNS_CHANNEL, FINAL_NAMES_COLUMNS_CHANNEL)


if __name__ == "__main__":
    main()
