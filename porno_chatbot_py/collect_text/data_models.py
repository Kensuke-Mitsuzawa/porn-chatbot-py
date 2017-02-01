import time
import os
import sqlite3
import traceback
from typing import List, Tuple
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
                 table_name_text:str="text"):
        self.table_name_links = table_name_links
        self.table_name_text = table_name_text
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

    def get_un_processed_story_link(self)->List[str]:
        """* What you can do
        - You can get link to story which not fetched yet.
        """
        sql_ = "SELECT url FROM {} WHERE status = ?".format(self.table_name_links)
        cur = self.db_connection.cursor()
        cur.execute(sql_, (False,))
        seq_urls = [record_tuple[0] for record_tuple in cur]

        return seq_urls

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






