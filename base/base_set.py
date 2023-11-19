import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
import time

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
# category_code 데이터 추가 위한 초기 세팅값
codes = {
    '식비': '0005;1056',
    '패션/쇼핑': '0006;1058;0016',
    '의료/건강': '0009;0014;1074;1066',
    '전기/전자': '0013;1151;1156',
    '금융': '0021;0022;0024;0025;1031',
    '생활': '0026;1077',
    '문화/여가': '1037;1152;1154',
    '교통': '1075;1029;0019',
    '여행/숙박': '0026',
    '교육/학습': '0026;1063;1062',
}

# category_code 저장을 위한 df 생성
def category_table():
    data = []
    for key, value in codes.items():
      code_list = value.split(';')

      for code in code_list:
        data.append({'c_code': code, 'category': key})

    df = pd.DataFrame(data)

    return df

# ----------------------------------------------


# category_code 설정 - 초기 1회 -> 이후 실행시 중복 오류
def set_category_code():
    # df 데이터 받아오기
    df_category = category_table()

    # category_code 저장
    df_category.to_sql(name='category_code', con=engine, if_exists='append', index=False)

    print("데이터 저장 완료")


# 초기 주식 데이터 저장
# stock_category 저장
def set_stock_category():
    # category_code 조회
    select_category_query = "select c_code from category_code"
    result = pd.read_sql_query(select_category_query, engine)

    # 업종 코드별 종목 조회
    for index, row in result.iterrows():
        c_code = row['c_code']

        # 응답 - 업종 코드에 대한 종목코드, 이름
        response = requests.get(indi_url + f'/stock/category/{c_code}')
        if response.status_code == 200:
            data = response.json()
            print(f"c_code: {c_code}")
            print(data['result'])
        else:
            print("error")
        time.sleep(1)
# stock 정보 저장


if __name__ == "__main__":
    # set_category_code()
    set_stock_category()
