from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when, date_format, regexp_replace, concat, to_date, lpad, bround, lit
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")\
    .config("spark.sql.streaming.streamingTimeout", "600000") \
    .getOrCreate()
spark.conf.set("spark.sql.streaming.trigger.interval", "2s")  # Điều chỉnh thời gian trigger
spark.conf.set("spark.streaming.backpressure.enabled", "true")  # Cho phép backpressure khi cần
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")  # Giới hạn tốc độ tiêu thụ từ Kafka
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
                                            .option("subscribe", "test") \
                                              .option("startingOffsets", "latest").load()

csv_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Tách dữ liệu CSV thành các cột 
processed_df = csv_df.select(split(csv_df['value'], ',').alias('columns'))

# Chuyển thành các cột riêng biệt
final_df = processed_df.selectExpr("columns[0] AS User", "columns[1] AS Card", "columns[2] AS Year", "columns[3] AS Month",
                                   "columns[4] AS Day", "columns[5] AS Time", "columns[6] AS Amount", 
                                   "columns[7] AS UseChip", "columns[8] AS MerchantName", "columns[9] AS MerchantCity", 
                                   "columns[10] AS MerchantState", "columns[11] AS Zip", "columns[12] AS MCC",
                                   "columns[13] AS Errors", "columns[14] AS IsFraud")

# Xử lý IsFraud = Yes (Loại bỏ các giao dịch gian lận)
final_df_filtered = final_df.filter(final_df['IsFraud'] == 'No')

# Chuyển đổi số tiền sang VNĐ (giả sử tỷ giá là 1 USD = 25370 VND)
final_df_filtered = final_df_filtered.withColumn(
    "Amount", 
    bround((regexp_replace(col("Amount"), "[$,]", "").cast(DoubleType()) * 25370), 2)
)

# Chuyển đổi ngày và giờ theo định dạng yêu cầu (dd/mm/yyyy và hh:mm:ss)
final_df_filtered = final_df_filtered.withColumn(
    "Time",
    concat(
        lpad(col("Time"), 5, "0"),  # Lpad phần giờ và phút
        lit(":00")  # Dùng `lit()` để thêm chuỗi ":00"
    )
)
final_df_filtered = final_df_filtered.withColumn(
    "Date",
    date_format(
        to_date(
            concat(
                lpad(col("Day").cast("string"), 2, "0"),  # Đảm bảo ngày có 2 chữ số
                lit("/"),  
                lpad(col("Month").cast("string"), 2, "0"),  # Đảm bảo tháng có 2 chữ số
                lit("/"),  
                col("Year").cast("string")  
            ), "dd/MM/yyyy"
        ), "dd/MM/yyyy"  # Đảm bảo định dạng cuối cùng là dd/MM/yyyy
    )
)

# Lưu kết quả vào HDFS hoặc bất kỳ hệ thống tệp phân tán nào
try:
    query = final_df_filtered.coalesce(1).writeStream.outputMode("append").format("csv") \
        .option("path", "/credit_card_output") \
        .option("checkpointLocation", "/credit_card_checkpoint").start()

    query.awaitTermination(60)

except Exception as e:
    print(f"An error occurred: {e}")
    query.awaitTermination()  

finally:
    query.stop()  