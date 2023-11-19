import pandas as pd
import pymysql
from sqlalchemy import create_engine
from tqdm import tqdm
import os
from dotenv import load_dotenv

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

# 데이터프레임 생성 (앞서 생성한 df를 사용)
# df = pd.DataFrame(data)

# MySQL 연결 시도
try:
    # MySQL 연결
    engine = create_engine(db_connection_str)
    connection = engine.connect()

    print("MySQL 연결 성공!")

    query = "select * from user"
    result_df = pd.read_sql_query(query, connection)
    print(result_df)

except Exception as e:
    print(f"MySQL 연결 오류: {e}")

finally:
    # 연결 종료
    if connection is not None:
        connection.close()
