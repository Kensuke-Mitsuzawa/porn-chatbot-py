from typing import List, Tuple
import json
import time
import os
import sqlite3
import traceback
from datetime import datetime
from porno_chatbot_py import logger_unit
logger = logger_unit.logger


class ScrapedPageObject(object):
    """Class object for saving page which already saved"""
    def __init__(self,
                 url_link:str,
                 title:str,
                 text:str,
                 author:str,
                 timestamp:datetime=datetime.now(),
                 updated_at:datetime=datetime.now()):
        self.title = title
        self.author = author
        self.url_link = url_link
        self.text = text
        self.timestamp = timestamp
        self.updated_at = updated_at


class CandidateLinkObject(object):
    """Class object for saving page to link and it's status"""
    def __init__(self,
                 url_link:str,
                 status:bool,
                 timestamp:datetime=datetime.now(),
                 updated_at:datetime=datetime.now()):
        self.url_link = url_link
        self.status = status
        self.timestamp = timestamp
        self.updated_at = updated_at



class SqliteDbHandler(object):
    def __init__(self,
                 path_sqlite_file:str,
                 table_name_links:str="links",
                 table_name_text:str="text",
                 table_name_training_pair="training_pair"):
        self.table_name_links = table_name_links
        self.table_name_text = table_name_text
        self.table_name_training_pair = table_name_training_pair
        if not os.path.exists(path_sqlite_file):
            self.db_connection = sqlite3.connect(database=path_sqlite_file)
            self.create_db()
        else:
            self.db_connection = sqlite3.connect(database=path_sqlite_file)

    def __del__(self):
        self.db_connection.close()

    def create_db(self):
        cur = self.db_connection.cursor()
        sql = """create table if not exists {table_name} (
            url TEXT PRIMARY KEY,
            status BOOLEAN,
            created_at DATETIME,
            updated_at DATETIME)"""
        cur.execute(sql.format(table_name=self.table_name_links))
        self.db_connection.commit()

        cur = self.db_connection.cursor()
        sql = """create table if not exists {table_name} (
        url TEXT PRIMARY KEY,
        novel_text TEXT,
        title TEXT,
        author TEXT,
        created_at DATETIME,
        updated_at DATETIME)"""
        cur.execute(sql.format(table_name=self.table_name_text))
        self.db_connection.commit()

        cur = self.db_connection.cursor()
        sql = """create table if not exists {table_name} (
        url TEXT,
        text_pair BLOB)"""
        cur.execute(sql.format(table_name=self.table_name_training_pair))
        self.db_connection.commit()
        cur.close()


    def insert_candidate_link(self, candidate_link_obj:CandidateLinkObject)->bool:
        """* What you can do
        - It saves candidate link into DB
        """
        check_sql = "SELECT count(url) FROM {} WHERE url = ?".format(self.table_name_links)
        cur = self.db_connection.cursor()
        cur.execute(check_sql, (candidate_link_obj.url_link, ))
        result = cur.fetchone()
        if result[0] >= 1:
            return False
        else:
            cur = self.db_connection.cursor()
            insert_sql = "INSERT INTO {}(url, status, created_at, updated_at) values (?, ?, ?, ?)".format(self.table_name_links)
            try:
                cur.execute(insert_sql, (candidate_link_obj.url_link,
                                     candidate_link_obj.status,
                                     candidate_link_obj.timestamp,
                                     candidate_link_obj.updated_at))
                self.db_connection.commit()
            except:
                logger.error(traceback.format_exc())
                self.db_connection.rollback()
                return False

            return True

    def insert_novel_story_text(self, scraped_obj:ScrapedPageObject)->bool:
        sql_check = "SELECT count(url) FROM {} WHERE url = ?".format(self.table_name_text)
        cur = self.db_connection.cursor()
        cur.execute(sql_check, (scraped_obj.url_link,))
        if cur.fetchone()[0] >= 1:
            cur.close()
            return False
        else:
            sql_insert = "INSERT INTO {}(url, novel_text, title, author, created_at, updated_at) values (?, ?, ?, ?, ?, ?)".format(self.table_name_text)
            cur = self.db_connection.cursor()
            try:
                cur.execute(sql_insert, (scraped_obj.url_link,
                                     scraped_obj.text,
                                     scraped_obj.title,
                                     scraped_obj.author,
                                     scraped_obj.timestamp,
                                     scraped_obj.updated_at))
                self.db_connection.commit()
                cur.close()
            except:
                logger.error(traceback.format_exc())
                self.db_connection.rollback()
                return False
            else:
                # linkテーブルのstatusを変更
                sql_update = "UPDATE {} SET status = ? WHERE url = ?".format(self.table_name_links)
                cur = self.db_connection.cursor()
                cur.execute(sql_update, (True,scraped_obj.url_link))
                self.db_connection.commit()
                cur.close()

                return True

    def insert_training_pair(self, processed_text_obj)->bool:
        seq_record_to_insert = []
        ### training_pairでレコードの存在を確認 ###
        for training_pair  in processed_text_obj.seq_conversation_obj:
            sql_check = "SELECT count(url) FROM {} WHERE url = ? AND text_pair =?".format(self.table_name_training_pair)
            cur = self.db_connection.cursor()
            cur.execute(sql_check, (processed_text_obj.scraped_obj.url_link, training_pair.to_json()))
            if cur.fetchone()[0] >= 1:
                cur.close()
                continue
            else:
                seq_record_to_insert.append(training_pair)

        sql_insert = "INSERT INTO {}(url, text_pair) values(?, ?)".format(self.table_name_training_pair)
        for training_pair in seq_record_to_insert:
            cur = self.db_connection.cursor()
            try:
                cur.execute(sql_insert, (processed_text_obj.scraped_obj.url_link, training_pair.to_json()))
                self.db_connection.commit()
                cur.close()
            except:
                logger.error(traceback.format_exc())
                self.db_connection.rollback()
        return True

    def get_un_processed_story_link(self)->List[str]:
        """* What you can do
        - You can get link to story which not fetched yet.
        """
        sql_ = "SELECT url FROM {} WHERE status = ?".format(self.table_name_links)
        cur = self.db_connection.cursor()
        cur.execute(sql_, (False,))
        seq_urls = [record_tuple[0] for record_tuple in cur]

        return seq_urls

    def get_text_data(self)->List[ScrapedPageObject]:
        sql_ = "SELECT url, novel_text, title, author, created_at, updated_at FROM {}".format(self.table_name_text)
        cur = self.db_connection.cursor()
        cur.execute(sql_)
        seq_urls = [
            ScrapedPageObject(
                url_link=record_tuple[0],
                title=record_tuple[2],
                text=record_tuple[1],
                author=record_tuple[3],
                timestamp=record_tuple[4],
                updated_at=record_tuple[5]
            ) for record_tuple in cur]
        return seq_urls


class ConversationPair(object):
    """It keeps pair of conversation"""
    def __init__(self,
                 conversation_text_a:str,
                 conversation_text_b:str):
        self.conversation_text_a = conversation_text_a
        self.conversation_text_b = conversation_text_b

    def to_json(self):
        return json.dumps([self.conversation_text_a, self.conversation_text_b], ensure_ascii=False)


class ProcessedTextObject(object):
    def __init__(self,
                 scraped_obj:ScrapedPageObject,
                 seq_conversation_obj:List[ConversationPair]):
        self.scraped_obj = scraped_obj
        self.seq_conversation_obj = seq_conversation_obj