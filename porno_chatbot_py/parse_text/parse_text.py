from porno_chatbot_py.data_models import ConversationPair, ProcessedTextObject, ScrapedPageObject, SqliteDbHandler
from porno_chatbot_py import logger_unit
import re
logger = logger_unit.logger


def extract_conversation_pairs(scraped_obj:ScrapedPageObject,
                               compiled_regular_expression_pattern)->ProcessedTextObject:
    """* What you can do
    - It extracts pair of conversation with Regular expression
    """
    seq_pair_obj = []
    for conversation_pair in compiled_regular_expression_pattern.findall(scraped_obj.text):
        seq_conversation_pair = conversation_pair.strip().split('」')
        seq_conv_pair = [string.replace('「', '') for string in seq_conversation_pair if string != '']
        if not len(seq_conv_pair) == 2:
            logger.error('')
        else:
            seq_pair_obj.append(ConversationPair(
                conversation_text_a=seq_conv_pair[0],
                conversation_text_b=seq_conv_pair[1])
            )

    return ProcessedTextObject(scraped_obj=scraped_obj, seq_conversation_obj=seq_pair_obj)


def main(db_handler:SqliteDbHandler):
    seq_scraped_obj = db_handler.get_text_data()
    regular_expression_pattern = re.compile(r'(「[^」]+」{1,}[^「]{,5}「[^」]+」{1,})')
    seq_processed_obj = [extract_conversation_pairs(scraped_obj, regular_expression_pattern) for scraped_obj in seq_scraped_obj]
    for processed_obj in seq_processed_obj:
        db_handler.insert_training_pair(processed_text_obj=processed_obj)


def test():
    path_text_db = '../../models/text-collection/extracted-text.sqlite3'
    db_handler = SqliteDbHandler(path_text_db)
    main(db_handler)


if __name__ == '__main__':
    test()