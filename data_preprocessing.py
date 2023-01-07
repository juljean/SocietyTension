import logging
from re import sub
import emot
import constants
import fetch
import stemmer

# Creating basic objects
logging.basicConfig(format="%(levelname)s - %(asctime)s: %(message)s", datefmt='%H:%M:%S', level=logging.INFO)
emot_object = emot.core.emot()


def text_to_word_list(text):
    """
    Preprocessing steps:
    - Lowercase text
    - Drop numbers
    - Verbalize emojis
    - Verbalize '+'
    - Drop words with length<2
    :param text:
    :return:
    """
    text = str(text)
    text = text.lower()
    detected_emojis = emot_object.emoji(text)
    if detected_emojis['flag']:
        text += str(detected_emojis['mean'])
    # Clean the text
    text = sub(r"[^A-Za-zА-Яа-яЄєЇїІі^/_+]", " ", text)
    text = sub(r"\+", " плюс ", text)
    text = sub(r"\s{2,}", " ", text)
    text = text.split()
    text_without_stopwords = []
    for word in text:
        # word = stemmer.stemWord(word)
        if word not in constants.STOP_WORDS:
            text_without_stopwords.append(word)
    return ' '.join(text_without_stopwords)


def process_comments(video_id, fetch_parameter="comments"):
    df = fetch.accessAPI(video_id, fetch_parameter)
    df_cleaned = df.dropna().drop_duplicates().reset_index(drop=True)
    for comment in df_cleaned["text_processed"]:
        df_cleaned.loc[df_cleaned["text_processed"] == comment, ["text_processed"]] = text_to_word_list(comment)
    preprocessed_df = df_cleaned.dropna().drop_duplicates().reset_index(drop=True)
    return preprocessed_df


if __name__ == "__main__":
    process_comments(constants.VIDEO_ID, "comments")
