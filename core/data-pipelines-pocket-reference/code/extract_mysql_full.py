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
