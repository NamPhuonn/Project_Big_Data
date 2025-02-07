from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Tạo SparkSession với hỗ trợ Hive
spark = SparkSession.builder \
    .appName("MergeCSVWithSchema") \
    .enableHiveSupport() \
    .getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("User", IntegerType(), True),
    StructField("Card", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", FloatType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", FloatType(), True),
    StructField("MCC", StringType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True),
    StructField("Date", StringType(), True)
])

# Đường dẫn input và output
input_path = "/credit_card_output"  # Thư mục chứa các file CSV
output_path = "/merged_credit_card_output"  # Thư mục lưu file hợp nhất

# Đọc dữ liệu với schema định nghĩa
all_csv_df = spark.read.csv(input_path, schema=schema, header=False)  # header=False vì file không có tiêu đề

# # Ghi dữ liệu thành một file duy nhất
all_csv_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

print(f"Data merged and saved to Hive table: {output_path}")