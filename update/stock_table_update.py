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
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_name = os.getenv('db_name')

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

# ----------------------------------------------

# stock 정보 저장
def set_stock():
    with engine.connect() as connection:
        # 종목 코드 조회 쿼리
        stock_code_query = "select stock_code from stock"
        stock_list = pd.read_sql(stock_code_query, engine)

        print(stock_list)
        for index, row in stock_list.iterrows():
            stock_code = row['stock_code']
            response = requests.get(indi_url + f'/stock/info/{stock_code}')
            data = response.json()
            if response.status_code == 200 and data['status'] == 200:
                print(data)
                price = data['result'][0]['y_close']
                day_range = data['result'][0]['day_range']
                market_cap = data['result'][0]['market_cap']
                query = text("UPDATE stock SET day_range = :day_range, market_cap = :market_cap, y_close = :price, update_date = :update_date WHERE stock_code = :stock_code")
                connection.execute(query, {"stock_code" : stock_code, "day_range" : day_range, "market_cap" : market_cap, "price" : price, "update_date" : today})
        connection.commit()
if __name__ == "__main__":
    set_stock()