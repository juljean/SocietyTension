import logging
from re import sub
import emot
import constants
import db_connection
import fetch
from langdetect import detect_langs
import spacy

nlp = spacy.load("uk_core_news_sm")


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
    detected_emojis = emot_object.emoji(text)
    if detected_emojis['flag']:
        text += str(detected_emojis['mean'])
    text = text.lower()
    # Clean the text
    text = sub(r"[^A-Za-zА-Яа-яЄєЇїІі^/_+]", " ", text)
    text = sub(r"\+", " плюс ", text)
    text = sub(r"\s{2,}", " ", text)
    try:
        recognized_language = detect_langs(text)[0].lang
    except Exception:
        recognized_language = 'no'

    text = text.split()
    text_without_stopwords = []
    for word in text:
        # word = stemmer.stemWord(word)
        word = lemmatize_text(word)
        if word not in constants.STOP_WORDS:
            text_without_stopwords.append(word)
    return ' '.join(text_without_stopwords), recognized_language


def lemmatize_text(text):
    processed_sentence = " ".join([word.lemma_ for word in nlp(text)])
    return processed_sentence


def process_comments(video_id, fetch_parameter="comments"):
    df = fetch.accessAPI(video_id=video_id, fetch_parameter=fetch_parameter)
    if df is not None:
        df_cleaned = df.dropna().drop_duplicates().reset_index(drop=True)
        for comment in df_cleaned["text_processed"]:
            text_processed, comment_language = text_to_word_list(comment)
            df_cleaned.loc[df_cleaned["text_processed"] == comment, ["text_processed"]] = text_processed
            df_cleaned.loc[df_cleaned["text_processed"] == text_processed, ["original_language"]] = comment_language
        preprocessed_df = df_cleaned.dropna().drop_duplicates().reset_index(drop=True)
        return preprocessed_df
    return None



def language_recognition(comments_list):
    language_data = {}
    for comment in comments_list:
        try:
            language_data[comment[0]] = detect_langs(comment[1])[0].lang
        except Exception as err:
            language_data[comment[0]] = 'no'
    return language_data


# if __name__ == "__main__":
    # process_comments(constants.VIDEO_ID, "comments")
