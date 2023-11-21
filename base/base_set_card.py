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
    select_user_query = "select user_seq from user"
    user_data = pd.read_sql_query(select_user_query, engine)

    data = []
    for index, row in user_data.iterrows():
        data.append({})
        card_i = index % 3
        data[index]['user_seq'] = row['user_seq'] # 유저 구
        data[index]['card_type'] = 0 # 카드 유형
        data[index]['card_name'] = card_name_list[card_i] # 카드 이름

    df = pd.DataFrame(data)
    print(data)
    df.to_sql('card', con=engine, if_exists='append', index=False)


# card_history table세팅 메서드
def set_card_history():
    select_card_query = "select card_seq from card"
    card_data = pd.read_sql_query(select_card_query, engine)

    # 유저 데이터용 - 카드 시퀀스 index가 짝수인경우 1번 데이터, 아닌경우 2번 데이터
    for index, row in card_data.iterrows():
        # 내역 데이터 갯수만큼 반복
        d_id = 1 if index % 2 == 0 else 2
        df = pd.read_csv(f'user{d_id}.csv', encoding='cp949') # 읽어올 파일 -> 1년치 데이터
        df = df.rename(columns={'업종': 'payment_category', '가맹점명': 'payment_detail', '승인금액' : 'payment_price', '승인일자' : 'payment_date'})
        selected_columns = ['payment_category', 'payment_detail', 'payment_price', 'payment_date']
        df_selected = df[selected_columns]
        df_selected['card_seq'] = row['card_seq']

        df_selected.to_sql('card_history', con=engine, if_exists='append', index=False)
        print(df_selected)


if __name__ == "__main__":
    # set_card()
    set_card_history()