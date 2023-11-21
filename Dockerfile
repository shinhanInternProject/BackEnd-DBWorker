# 베이스 이미지 선택
FROM --platform=linux/amd64 python:3.8

# 작업 디렉토리 설정
WORKDIR /update

# requirements.txt 파일 복사
COPY requirements.txt /update/
COPY .env /update/
# requirements.txt에 기반한 패키지 설치
RUN pip install -r requirements.txt

# 스크립트 파일 복사
COPY update/stock_table_update.py /update/

# 실행 명령 지정
CMD ["python", "stock_table_update.py"]