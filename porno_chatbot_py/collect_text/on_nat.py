from porno_chatbot_py import data_models
from porno_chatbot_py import logger_unit
from bs4 import BeautifulSoup
from typing import List, Tuple, Dict, Any
from itertools import chain
import requests
import re
import os
import time
logger = logger_unit.logger


def parse_top_page(top_page_html:str)->List[str]:
    """* What you can do
    - It parse top page and get link to each novel top page

    * Output
    - list of URL link into each novel category
    """
    soup = BeautifulSoup(top_page_html, "lxml")
    link_nodes = soup.find_all("a", href=re.compile("bbs/s/log_novel"))
    seq_link_text = [node.get("href") for node in link_nodes]

    return seq_link_text


def parse_category_top_page(category_top_page_html:str)->List[data_models.CandidateLinkObject]:
    """* What you can do
    - It parse each category top page & get link to each novel story
    """
    soup = BeautifulSoup(category_top_page_html, "lxml")
    link_nodes = soup.find_all("a", href=re.compile("novel/bbs/s/msg_novel"))
    seq_link_text = [data_models.CandidateLinkObject(url_link=url.get('href').strip(), status=False) for url in link_nodes]

    return seq_link_text


def get_link_to_story(db_handler:data_models.SqliteDbHandler,
                      target_url:str,
                      time_sleep:int=2,
                      is_test:bool=False):
    """* What you can do
    - It extracts link to novel story
    """
    top_page_html = requests.get(target_url).text
    seq_link_text = parse_top_page(top_page_html)

    seq_category_top_page_html_text = []
    for i, link in enumerate(seq_link_text):
        seq_category_top_page_html_text.append(requests.get(os.path.join(target_url, link)).text)
        time.sleep(time_sleep)
        if is_test and i == 3: break
    ### It parses each category top page & take link to each nove story ###
    seq_link_candidate_obj = chain.from_iterable([parse_category_top_page(category_page_html)
                              for category_page_html in seq_category_top_page_html_text])
    for candidate_obj in seq_link_candidate_obj:
        db_handler.insert_candidate_link(candidate_link_obj=candidate_obj)
        logger.info(msg='Saved url={}'.format(candidate_obj.url_link))


def fetch_and_parse_story_page(url_story_page:str)->data_models.ScrapedPageObject:
    """* What you can do
    - It downloads text data from url, and gets only text
    """
    story_page_html = requests.get(url_story_page).text
    soup = BeautifulSoup(story_page_html, "lxml")
    link_nodes = soup.find_all("div", class_="reslist_main")
    text_story = '\n'.join([node.text.strip() for node in link_nodes])

    title_node = soup.find('span', style='padding-right:8px;font-size:medium;font-weight:bold;')
    if not title_node is None:
        title = title_node.text.strip()
    else:
        title = None

    author_node = soup.find("div", style='font-size:11px;margin-top:10px;').find('a')
    if not author_node is None:
        author = author_node.text.strip()
    else:
        author = None

    return data_models.ScrapedPageObject(
        url_link=url_story_page,
        title=title,
        text=text_story,
        author=author)


def get_text_data_story(db_handler:data_models.SqliteDbHandler, time_sleep:int=3):
    """* What you can do
    - It finds URL which is not processed yet
    - It downloads text and gets text data
    - It saves text data into DB
    """
    seq_links_story_un_processed = db_handler.get_un_processed_story_link()
    for url_story in seq_links_story_un_processed:
        scraped_obj = fetch_and_parse_story_page(url_story)
        db_handler.insert_novel_story_text(scraped_obj=scraped_obj)
        logger.info(msg='Saved url={}'.format(scraped_obj.url_link))
        time.sleep(time_sleep)


def test():

    db_handler = data_models.SqliteDbHandler(path_sqlite_file='./extracted-text.sqlite3')

    url_top_page = 'http://nan-net.com/novel/'
    get_link_to_story(db_handler, url_top_page, time_sleep=3, is_test=False)
    get_text_data_story(db_handler)

if __name__ == '__main__':
    test()