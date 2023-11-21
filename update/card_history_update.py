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
def update_card():

    data = []
    for i in range(5):
        data.append({})
        card_i = i % 3
        data[i]['card_type'] = 0 # 카드 유형
        data[i]['card_name'] = card_name_list[card_i] # 카드 이름

    df = pd.DataFrame(data)
    print(data)
    df.to_sql('card', con=engine, if_exists='append', index=False)


# card_history table세팅 메서드
def update_card_history():
    # card_seq user_seq없는 값
    select_card_query = "select card_seq from card where user_seq IS NULL"
    card_data = pd.read_sql_query(select_card_query, engine)

    for index, row in card_data.iterrows():
        card_seq = row['card_seq']
        # index가 짝수인경우 1번 데이터, 아닌경우 2번 데이터        # 내역 데이터 갯수만큼 반복
        d_id = 1 if card_seq % 2 == 0 else 2
        df = pd.read_csv(f'../base/user{d_id}.csv', encoding='cp949') # 읽어올 파일 -> 1년치 데이터
        df = df.rename(columns={'업종': 'payment_category', '가맹점명': 'payment_detail', '승인금액' : 'payment_price', '승인일자' : 'payment_date'})
        selected_columns = ['payment_category', 'payment_detail', 'payment_price', 'payment_date']
        df_selected = df[selected_columns]
        df_selected['card_seq'] = card_seq
    #
        df_selected.to_sql('card_history', con=engine, if_exists='append', index=False)
        print(card_seq, df_selected)


if __name__ == "__main__":
    # update_card()
    update_card_history()