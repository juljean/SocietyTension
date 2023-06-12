from re import sub
import emot
import constants
import yt_api_data_fetch
from langdetect import detect_langs
import spacy

nlp = spacy.load("uk_core_news_sm")
emot_object = emot.core.emot()


def text_to_word_list(text):
    """
    Preprocessing steps:
    - Lowercase text
    - Drop numbers
    - Verbalize emojis
    - Verbalize '+'
    - Drop words with length < 2
    :param text: Input text to be preprocessed
    :return: Preprocessed text and recognized language
    """

    # Convert text to string
    text = str(text)

    # Detect emojis in the text and append their verbalized form
    detected_emojis = emot_object.emoji(text)
    if detected_emojis['flag']:
        text += str(detected_emojis['mean'])

    # Convert the text to lowercase
    text = text.lower()

    # Cleansing of the text data using regular expressions
    text = sub(r"[^A-Za-zА-Яа-яЄєЇїІі^/_+]", " ", text)
    text = sub(r"\+", " плюс ", text)
    text = sub(r"\s{2,}", " ", text)

    # Detect the language of the text
    try:
        recognized_language = detect_langs(text)[0].lang
    except Exception:
        recognized_language = 'no'

    # Split the text into individual words
    text = text.split()

    # Lemmatize each word and remove stopwords
    text_without_stopwords = []
    for word in text:
        word = lemmatize_text(word)
        if word not in constants.STOP_WORDS:
            text_without_stopwords.append(word)

    # Join the preprocessed words back into a string
    return ' '.join(text_without_stopwords), recognized_language


def lemmatize_text(text):
    """
    Lemmatize the input text using the nlp object
    :param text: Input text to be lemmatized
    :return: Processed sentence with lemmatized words
    """
    processed_sentence = " ".join([word.lemma_ for word in nlp(text)])
    return processed_sentence


def process_comments(video_id, fetch_parameter="comments"):
    """
    Process the comments for a given video ID
    :param video_id: ID of the video
    :param fetch_parameter: Parameter for data fetching (default: "comments")
    :return: Preprocessed DataFrame of comments
    """
    # Fetch the data using yt_api_data_fetch
    df = yt_api_data_fetch.accessAPI(video_id=video_id, data_fetch_parameter=fetch_parameter)

    if df is not None:
        # Drop NaN values and duplicates from the DataFrame
        df_cleaned = df.dropna().drop_duplicates().reset_index(drop=True)
        print(df_cleaned)

        # Process each comment
        for comment in df_cleaned["text_processed"]:
            # Preprocess the comment using text_to_word_list
            text_processed, comment_language = text_to_word_list(comment)

            # Update the processed text and language in the DataFrame
            df_cleaned.loc[df_cleaned["text_processed"] == comment, ["text_processed"]] = text_processed
            df_cleaned.loc[df_cleaned["text_processed"] == text_processed, ["original_language"]] = comment_language

        # Drop NaN values and duplicates again from the DataFrame
        preprocessed_df = df_cleaned.dropna().drop_duplicates().reset_index(drop=True)
        return preprocessed_df

    return None


def language_recognition(comments_list):
    """
    Recognizing language of the comment. If unknown - set 'no'
    :param comments_list: list of comment for language recognition
    :return: dict with comment and corresponding language
    """
    comment_language_dict = {}
    for comment in comments_list:
        try:
            comment_language_dict[comment[0]] = detect_langs(comment[1])[0].lang
        except Exception as err:
            comment_language_dict[comment[0]] = 'no'
    return comment_language_dict


# if __name__ == "__main__":
    # process_comments(constants.VIDEO_ID, "comments")
