# Forensic Lab 5: Delta Log Corruption & OCC Collision

## 1. Objectives (Mục Tiêu Phân Tích)
- [ ] Tái hiện hiện tượng xung đột ghi đồng thời (Concurrent Modification Exception) do cơ chế Optimistic Concurrency Control (OCC).
- [ ] Mô phỏng sự cố hỏng hóc tệp Transaction Log (`_delta_log`) và quan sát chuỗi phản ứng dây chuyền làm sập hệ thống đọc.
- [ ] Triển khai các giao thức khôi phục (Recovery protocols) như `FSCK REPAIR TABLE` và Rollback thủ công.

## 2. Incident Scenario (Hiện Trường Sự Cố)
Trung tâm vận hành (NOC) báo cáo hai sự cố nghiêm trọng trên cụm Data Lakehouse:
- **Sự cố 1 (Xung Đột OCC):** Hai tiến trình Spark độc lập thực thi thao tác Update trên cùng một bảng Khách Hàng. Một tiến trình hoàn tất, tiến trình còn lại bị hệ thống từ chối với ngoại lệ `ConcurrentModificationException`.
- **Sự cố 2 (Bất Động Hệ Thống):** Quá trình tự động hóa dọn dẹp vô tình xóa tệp `00000000000000000001.json` trong thư mục `_delta_log`. Mặc dù 100% dữ liệu Parquet vật lý vẫn nguyên vẹn trên S3, toàn bộ các Job đọc bảng này lập tức ngừng hoạt động với lỗi `FileNotFoundException`.

## 3. Lab Setup (The Crime Scene)

Đoạn mã sau thiết lập bảng Delta và chuẩn bị môi trường cho các sự cố.

### The OCC Collision (Mô Phỏng Xung Đột Đồng Thời)
Để tái hiện OCC, chúng ta thiết lập mã logic chuẩn bị cho hai giao dịch cập nhật. (Đã bổ sung đầy đủ thư viện phụ thuộc).

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Cấu hình Spark Engine tích hợp Delta Lake
builder = SparkSession.builder.appName("Forensic_Lab_5") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
table_path = "/tmp/delta_lab_table"

# Khởi tạo bảng Delta ban đầu
df = spark.range(1, 10).toDF("id").withColumn("status", F.lit("NEW"))
df.write.format("delta").mode("overwrite").save(table_path)

# KỊCH BẢN GIẢ LẬP XUNG ĐỘT:
# Job A và Job B đồng thời thực hiện lệnh Update sau đây:
deltaTable = DeltaTable.forPath(spark, table_path)

# Tiến trình A: Cập nhật id = 1
# Tiến trình B: Cập nhật id = 2
# Nếu cả hai ID cùng nằm trong một tập tin Parquet vật lý, hệ thống OCC sẽ kích hoạt.
deltaTable.update(
    condition="id = 1",
    set={"status": F.lit("DONE_A")}
)
```

### The Log Saboteur (Mô Phỏng Xóa Transaction Log)
Sau khi hoàn tất khởi tạo và cập nhật, hệ thống sinh ra `000000.json` và `000001.json`.
Sử dụng công cụ dòng lệnh (Terminal) để can thiệp vật lý:
```bash
# Mô phỏng sự cố mất mát siêu dữ liệu (Metadata Loss)
rm /tmp/delta_lab_table/_delta_log/00000000000000000001.json
```
Thực thi lệnh đọc từ PySpark:
```python
spark.read.format("delta").load(table_path).show()
```
**Kết quả (Crash):** Lỗi `java.lang.IllegalStateException: File ...001.json does not exist`. Toàn bộ quá trình quét bảng bị đình chỉ.

## 4. Forensic Analysis (Phân Tích Pháp Y)

### Bằng Chứng 1: Bản Chất Phòng Vệ Của OCC
Ngoại lệ `ConcurrentModificationException` không phải là lỗi hệ thống (Bug), mà là cơ chế bảo vệ tính toàn vẹn (ACID) của Delta Lake.
- **Quy trình OCC:** Job A và Job B cùng bắt đầu giao dịch. Job A hoàn tất trước và ghi Log số 01. Job B hoàn tất sau, tải Log số 01 về đối chiếu.
- **Điểm đụng độ (Collision):** Job B phát hiện ra cả hai tiến trình cùng cố gắng ghi đè lên **cùng một tệp Parquet vật lý**. Do thuật toán không thể an toàn trộn lẫn (Merge) dữ liệu nội tại ở cấp độ dòng (Row-level) trên cùng một tệp cột (Columnar file), hệ thống kích hoạt ngoại lệ từ chối Job B nhằm ngăn chặn Data Corruption.

### Bằng Chứng 2: Quyền Lực Tuyệt Đối Của Meta-data
Sự cố thứ hai chứng minh một nghịch lý vật lý: Sự tồn tại của các khối dữ liệu khổng lồ trên ổ cứng hoàn toàn vô nghĩa nếu thiếu vắng hệ thống Meta-data định tuyến (`_delta_log`).
Khi tệp JSON bị thiếu, Delta Engine mất khả năng tái tạo trạng thái Logic (Snapshot) của bảng, buộc phải ngắt kết nối để tránh trả về dữ liệu sai lệch.

## 5. Mitigation & Key Takeaways (Phác Đồ Điều Trị)

1. **Giảm Thiểu Rủi Ro OCC (Simplification):**
   - **Tối ưu Hashing/Partitioning:** Đảm bảo các tiến trình cập nhật song song (Ví dụ Job A và B) ghi vào các phân vùng (Partitions) vật lý khác nhau (Ví dụ: `date_id` khác nhau). OCC sẽ cho phép cả hai cùng Commit thành công.
   - **Tích hợp Retry Logic:** Đóng gói khối mã Update vào các vòng lặp thử lại (Retry block) với Backoff tuyến tính, giúp tiến trình bị từ chối tự động đọc lại trạng thái mới và ghi lại an toàn.

2. **Cấp Cứu Hỏng Hóc Delta Log:**
   - **Tái thiết Log (FSCK):** Sử dụng lệnh `FSCK REPAIR TABLE delta.'/tmp/delta_lab_table'`. Engine sẽ duyệt lại ổ đĩa vật lý, dọn dẹp các mục mồ côi (Orphaned files) và đồng bộ lại sổ cái (Ledger).
   - **Rollback Tĩnh:** Trong trường hợp nghiêm trọng, Kỹ sư xóa thủ công các file Log hỏng, lùi con trỏ (Pointer) về tệp Checkpoint Parquet gần nhất. Hành động này đồng nghĩa với việc chấp nhận mất một phần dữ liệu cập nhật mới nhất để hồi sinh toàn cục (Availability over Consistency trade-off).

3. **Bảo Mật Cơ Sở:** Áp dụng nguyên tắc Đặc quyền tối thiểu (Least Privilege). Chỉ cấp quyền Sửa/Xóa thư mục `_delta_log` cho tài khoản Service Account thực thi Spark, ngăn chặn mọi can thiệp bên ngoài.
