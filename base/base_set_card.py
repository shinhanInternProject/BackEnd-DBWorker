# 기본 db 세팅
# card / card_history

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
# db_host = os.getenv('db_host')
# db_port = os.getenv('db_port')
# db_name = os.getenv('db_name')
db_host = os.getenv('db_host_pub')
db_port = os.getenv('db_port_pub')
db_name = os.getenv('db_name_pub')

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

# 카드 이름 리스트
card_name_list = [
    '신한카드 Deep Dream Platinum+',
    '신한카드 Deep Oil',
    '신한카드 봄'
]

# ----------------------------------------------

# 데이터 세팅 메서드 리스트

# card table세팅 메서드
def set_card():
    data = []
    for i in range(len(card_name_list)):
        data.append({})
        data[i]['card_type'] = 0 # 카드 유형
        data[i]['card_name'] = card_name_list[i] # 카드 이름

    df = pd.DataFrame(data)
    print(data)
    df.to_sql('card', con=engine, if_exists='append', index=False)


# card_history table세팅 메서드
def set_card_history():
    for i in range(1, 3):
        df = pd.read_csv(f'user{i}.csv', encoding='cp949') # 읽어올 파일
        data = []
        for index, row in df.iterrows():
            data.append({'card_seq' : 1, 'payment_category' : row['업종'], 'payment_detail' : row['가맹점명'], 'payment_price' : row['승인금액'], 'payment_date' : row['승인일자']})
        df_his = pd.DataFrame(data)
        df_his.to_sql('card_history', con=engine, if_exists='append', index=False)
        print(df_his)


if __name__ == "__main__":
    # set_card()
    set_card_history()