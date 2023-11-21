# 기본 데이터 설정
# 1. card table data
# 2. card history table data

import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import pymysql
from sqlalchemy import create_engine

load_dotenv(verbose=True)

# MySQL 연결 정보
db_username = os.getenv('db_username')
db_password = os.getenv('db_password')
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_name = os.getenv('db_name')

pymysql.install_as_MySQLdb()

# MySQL 연결 문자열 생성
db_connection_str = f'mysql+mysqldb://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
connection = None

# ----------------------------------------------

# MySQL 연결 시도
try:
    # MySQL 연결
    engine = create_engine(db_connection_str)
    connection = engine.connect()

    print("MySQL 연결 성공!")

except Exception as e:
    print(f"MySQL 연결 오류: {e}")

finally:
    # 연결 종료
    if connection is not None:
        connection.close()


# ----------------------------------------------

# 데이터 세팅 메서드 리스트

# card table세팅 메서드
def set_card():
    pass


# card_history table세팅 메서드
def set_card_history():
    pass


if __name__ == "__main__":
    set_card()
    set_card_history()