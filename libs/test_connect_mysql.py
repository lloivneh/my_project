from pyspark.sql import SparkSession
import json
import os

# ===== Đọc config database =====
current_dir = os.path.dirname(__file__)
json_path = os.path.join(current_dir, "connect_mysql.json")

with open(json_path) as f:
    db_conf = json.load(f)

# ===== Tạo Spark Session =====
spark = SparkSession.builder \
    .appName("test_mysql_connection") \
    .getOrCreate()

jdbc_url = f"jdbc:mysql://{db_conf['host']}:{db_conf['port']}/{db_conf['database']}"

# ===== Thử đọc 1 table (hoặc có thể dùng SELECT 1) =====
try:
    test_df = spark.read.format("jdbc").option("url", jdbc_url) \
        .option("dbtable", "(SELECT 1) AS tmp") \
        .option("user", db_conf['username']) \
        .option("password", db_conf['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    test_df.show()
    print("✅ Kết nối MySQL bằng PySpark thành công!")

except Exception as e:
    print("❌ Lỗi kết nối với MySQL bằng PySpark:", e)

finally:
    spark.stop()