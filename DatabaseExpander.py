import pandas as pd
from sqlalchemy import create_engine
import random

# 사용자 입력을 받아 데이터베이스 설정
db_username = input("Enter your database username: ")
db_password = input("Enter your database password: ")
db_host = input("Enter your database host: ")
db_name = input("Enter your database name: ")
table_name = input("Enter your table name: ")

# 데이터베이스 연결
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_name}')

# 데이터 불러오기
df = pd.read_sql(f'SELECT * FROM {table_name}', engine)

# 데이터 복제 및 임의 변경
while df.memory_usage(deep=True).sum() < 100 * 1024**3:  # 100GB
    new_df = df.copy()

    # 필요한 값들을 임의로 변경 (여기서는 timestamp, latitude, longitude, forecastedTime)
    new_df['timestamp'] += random.randint(1, 100)
    new_df['latitude'] += random.uniform(-0.01, 0.01)
    new_df['longitude'] += random.uniform(-0.01, 0.01)
    new_df['forecastedTime'] += random.randint(1, 100)

    df = pd.concat([df, new_df])

# 결과 데이터를 데이터베이스에 쓰기
df.to_sql(table_name, engine, if_exists='append', index=False)
