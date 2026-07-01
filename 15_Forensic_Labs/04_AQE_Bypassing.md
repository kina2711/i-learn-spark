# Forensic Lab 4: Sự Bất Lực Của AQE Trước Null Skew

## 1. Objectives (Mục Tiêu Phân Tích)
- [ ] Kích hoạt Adaptive Query Execution (AQE) và chứng minh giới hạn của nó.
- [ ] Phân biệt rõ sự khác nhau giữa Cartesian Product (Tích Đề-các) và Shuffle Skew (Lệch phân vùng do Hashing) khi thực thi Null Join.
- [ ] Chẩn đoán hiện tượng OOM hoặc Straggler Task do tập trung các bản ghi NULL vào một phân vùng duy nhất.

## 2. Incident Scenario (Hiện Trường Sự Cố)
Hệ thống Data Warehouse cảnh báo một Job ETL báo cáo kẹt (Stuck) ở 99% suốt 3 giờ đồng hồ. Kỹ sư phân tích phản bác: *Tôi đã bật AQE Skew Join, tại sao hệ thống không tự động xử lý Data Skew?*. 
Kiểm tra Spark UI, 199 Tasks đã hoàn tất trong 1 phút, nhưng Task cuối cùng đang vật lộn với đĩa cứng (Massive Disk Spill).
Bản chất của vấn đề không phải AQE bị lỗi, mà do Kỹ sư không hiểu cơ chế vật lý của dữ liệu rỗng (NULL) khi đi qua hàm băm (Hash Function) và ranh giới giới hạn của thuật toán AQE.

## 3. Lab Setup (The Crime Scene)

Khởi tạo PySpark, kích hoạt AQE, và thiết lập một kịch bản Left Join nơi bảng chính (Fact table) chứa 99% giá trị `user_id` là NULL.

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Khởi tạo Session và ép ngưỡng AQE cực thấp để dễ kích hoạt
spark = SparkSession.builder \
    .appName("Forensic_Lab_4_Null_Skew") \
    .master("local[4]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1mb") \
    .getOrCreate()

# Bảng Trái (Fact): 1 Triệu dòng, 99% user_id là NULL
df_left = spark.range(0, 1000000).withColumn(
    "user_id", F.when(F.rand() < 0.99, F.lit(None)).otherwise(F.col("id"))
).withColumn("payload_left", F.expr("repeat('L', 100)"))

# Bảng Phải (Dimension): 5000 dòng chuẩn
df_right = spark.range(0, 5000).withColumn(
    "user_id", F.col("id")
).withColumn("payload_right", F.expr("repeat('R', 100)"))

# Kích nổ hệ thống bằng Left Join
print("=== Kích Hoạt Nút Thắt Null Skew ===")
df_join = df_left.join(df_right, "user_id", "left")

# Action bắt buộc để kích hoạt đồ thị DAG
df_join.write.format("noop").mode("overwrite").save()
```

## 4. Forensic Analysis (Phân Tích Pháp Y)

### Bằng Chứng 1: Điểm Nghẽn Hashing (Shuffle Skew)
Nhiều Kỹ sư lầm tưởng rằng `NULL` kết hợp với `NULL` sẽ sinh ra Tích Đề-các (Cartesian Product) khổng lồ. 
> [!IMPORTANT] Chân Lý Logic SQL
> Trong chuẩn SQL, `NULL = NULL` trả về `Unknown` (False). Do đó, Spark **không bao giờ** ghép các dòng NULL của bảng Trái với các dòng NULL của bảng Phải. Chúng hoàn toàn không sinh ra Cartesian Product.

**Tuy nhiên, rủi ro vật lý lại nằm ở khâu trung chuyển (Shuffle):**
Trước khi thực hiện Sort-Merge Join, Spark phải băm (Hash) cột `user_id` để phân phối dữ liệu qua các Partition. Hàm Hash của Spark tuân thủ quy tắc: **Tất cả các giá trị NULL đều trả về cùng một mã Hash cố định (Thường là Partition 0)**.
Kết quả: 990,000 dòng của bảng Trái đều đổ dồn về cùng một Task duy nhất. Task này cạn kiệt RAM, bắt buộc xả (Spill) xuống mặt đĩa cứng (Disk), gây ra hiện tượng Straggler Task (Chạy chậm bất thường).

### Bằng Chứng 2: AQE Bất Lực (Bypassing)
Tại sao AQE Skew Join không cứu được chúng ta?
Cơ chế của AQE là: Khi phát hiện Partition A của bảng Trái quá béo phì (Ví dụ chứa 990,000 dòng), nó sẽ chẻ Partition A thành 10 phần nhỏ. Để kết quả Join không bị sai, nó phải **nhân bản (Replicate)** Partition A của bảng Phải lên 10 lần.
- Tuy nhiên, Spark được lập trình để nhận biết rằng: **NULL sẽ không bao giờ khớp với bất kỳ thứ gì (Kể cả NULL khác)**.
- Do đó, việc AQE chia nhỏ khối NULL và nhân bản bảng Phải là một hành động vô nghĩa (No-op), tiêu tốn tài nguyên vô ích. Thuật toán AQE được thiết kế để **Bỏ qua (Bypass)** việc tối ưu hóa Skew Join đối với giá trị NULL. Hệ thống ngậm ngùi chấp nhận để Task đơn độc đó oằn mình gánh 990,000 bản ghi.

## 5. Mitigation & Key Takeaways (Phác Đồ Điều Trị)

Sự thông minh của Engine (AQE) không thể bù đắp cho sự lỏng lẻo trong việc làm sạch dữ liệu (Data Quality).
1. **Lọc Rác Thượng Nguồn (Upstream Filtering):** Nếu nghiệp vụ không yêu cầu giữ lại các bản ghi NULL (Ví dụ Inner Join), hãy chủ động xóa chúng trước khi quá trình Shuffle diễn ra.
   ```python
   # Chủ động triệt tiêu Skew trước khi đụng độ Shuffle
   df_clean = df_left.filter(F.col("user_id").isNotNull())
   df_join = df_clean.join(df_right, "user_id", "inner")
   ```
2. **Xử Lý Left Join (Tách Luồng):** Nếu bắt buộc dùng Left Join (Cần giữ lại dòng NULL của bảng Trái nhưng dữ liệu bảng Phải trả về Null), ta phải phân tách tập dữ liệu:
   - *Luồng 1:* Lọc các bản ghi `user_id IS NOT NULL` $\rightarrow$ Thực hiện Join bình thường (AQE sẽ hoạt động nếu có Skew).
   - *Luồng 2:* Lọc các bản ghi `user_id IS NULL` $\rightarrow$ Gắn thủ công các cột của bảng Phải thành rỗng mà KHÔNG CẦN JOIN.
   - Hợp nhất (Union) hai luồng lại.
3. **Triết Lý Thiết Kế:** Bạn không thể phụ thuộc vào một tham số cấu hình (Config) để giải quyết mọi bài toán vật lý. Tư duy Data Engineer Staff-Level đòi hỏi sự am hiểu hình thái dữ liệu (Data Skewness) trước khi nó tiến vào cỗ máy phân tán.
