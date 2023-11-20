# pipeline : Database(MySQL) > csv > S3

import pymysql
import csv
import boto3
import configparser

# Database to CSV

parser = configparser.ConfigParser()
parser.read("../config/pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port))

if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established!")

    m_query = "select * from orders"
    local_filename = "order_extract.csv"

    m_cursor = conn.cursor()
    m_cursor.execute(m_query)
    results = m_cursor.fetchall()

    with open(local_filename, 'w') as fp:
        csv_w = csv.writer(fp, delimiter='|')
        csv_w.writerows(results)

    fp.close()
    m_cursor.close()
    conn.close()

    # CSV to S3

    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")
    region_name = parser.get("aws_boto_credentials", "region_name")

    aws_session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )

    s3 = s3_client = aws_session.client('s3')
    s3_file = local_filename

    s3.upload_file(local_filename, bucket_name, s3_file)

# extract_mysql_incremental
# select t1.*
# from orders as t1
# where t1.last_updated > (select MAX(last_updated) from orders)

# 이진 로그 수집
# 1. 소스 데이터베이스 정보 copy to 데이터웨어하우스, (소스 테이블 lock, mysqldump -> 데이터웨어하우스에 로드)
# 2. 이진 로그를 사용하여 후속 변경 사항을 수집

# 이진 로그 활성화 확인 (ON or OFF)
# select variable_value as bin_log_status
# from performance_schema.global_variables
# where VARIABLE_NAME='log_bin'

# 이진 로그 format - status는 행동 (CUD)에 대해 SQL 문 자체를 기록
# select variable_value as bin_log_format
# from performance_schema.global_variables
# where VARIABLE_NAME='binlog_format'
