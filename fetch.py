import os
import json
import googleapiclient.discovery
import pandas as pd
import configs

VIDEO_ID = "ycV6bF_er3c"
REMAIN_COLUMNS = ["snippet.topLevelComment.id", "snippet.topLevelComment.snippet.videoId",
                  "snippet.topLevelComment.snippet.textOriginal",
                  "snippet.topLevelComment.snippet.authorDisplayName",
                  "snippet.topLevelComment.snippet.viewerRating,snippet.topLevelComment.snippet.likeCount",
                  "snippet.topLevelComment.snippet.publishedAt"]
FINAL_NAMES_COLUMNS = ['videoId','textOriginal', 'authorDisplayName', "likeCount", "publishedAt"]


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


def create_dataframe(json_object):
    df = pd.json_normalize(json_object)
    df.drop(columns=df.columns.difference(REMAIN_COLUMNS), axis=1, inplace=True)
    df.set_axis(FINAL_NAMES_COLUMNS, axis=1, inplace=True)
    print(df)
    df.to_csv("VideoCommentData/" + str(VIDEO_ID) + ".csv")


def main():
    json_object = fetch_comments()
    create_dataframe(json_object)


if __name__ == "__main__":
    main()
