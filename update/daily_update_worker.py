# 일간 데이터 업데이트 워커
# 업데이트 대상 - stock table's day_range, market_cap, y_close - 등락률, 시가총액, 전일종가

import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import text

load_dotenv(verbose=True)

# 환경 변수값 불러오기
indi_url = os.getenv("indi_api_url")

# MySQL 연결 정보
db_username = os.getenv('db_username')
db_password = os.getenv('db_password')
# db_host = os.getenv('db_host')
# db_port = os.getenv('db_port')
# db_name = os.getenv('db_name')

# public cloud test
db_host = os.getenv('db_host_pub')
db_port = os.getenv('db_port_pub')
db_name = os.getenv('db_name_pub')

# 조회를 위한 금일 날짜
today = datetime.now().strftime("%Y%m%d")

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
