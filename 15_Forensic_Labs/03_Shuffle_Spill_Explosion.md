# Forensic Lab 3: Shuffle Spill Explosion - Ác Mộng Cạn Kiệt Đĩa

## 1. Objectives (Mục Tiêu Pháp Y)
- [ ] Tái tạo một sự cố Spill ra Đĩa (Spill Disk) do Shuffle Data Skew.
- [ ] Quan sát sự khác biệt khổng lồ về mặt vật lý giữa `Spill (Memory)` và `Spill (Disk)`.
- [ ] Chứng minh rằng khi Spill Disk xảy ra, OOM sẽ không nổ ở RAM, mà Job sẽ chết vì No space left on device (Hết dung lượng đĩa vật lý).

## 2. Incident Scenario (Tình Huống Giả Lập)
Hệ thống báo động lúc 3h sáng. Một Job Spark đột nhiên chết với lỗi kỳ lạ: `java.io.IOException: No space left on device` hoặc Node bị đánh sập hoàn toàn. Kỹ sư trực ca bối rối: Rõ ràng Server cấp đến 128GB RAM cơ mà, sao lại báo đầy đĩa?. Bạn - một Staff Engineer - nhìn lướt qua Spark UI tab Stages và thấy một cột máu đỏ rực mang tên **Spill (Memory): 500GB / Spill (Disk): 50GB**. Bạn biết ngay thủ phạm là Sort-Merge Join kết hợp với một Skew Key siêu to khổng lồ đã nôn mửa (Spill) toàn bộ dữ liệu xuống đĩa cứng cục bộ, làm gặp sự cố nghiêm trọng ổ đĩa của Node.

## 3. Lab Setup (The Crime Scene)

Khởi tạo PySpark, ép cấu hình RAM siêu nhỏ và thu hẹp bộ đệm Spill để Spark buộc phải nôn mửa liên tục xuống đĩa cứng thay vì cố gồng trên RAM.

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Khởi tạo Spark với cấu hình "Ép Spill"
spark = SparkSession.builder \
    .appName("Forensic_Lab_3_Spill_Explosion") \
    .master("local[2]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.shuffle.file.buffer", "32k") \
    .config("spark.shuffle.spill.compress", "true") \
    .getOrCreate()

# Tạo DataFrame 1: Skew kịch liệt (90% dữ liệu mang id = 1)
df1 = spark.range(0, 1000000).withColumn(
    "id", F.when(F.rand() < 0.9, 1).otherwise(F.col("id"))
).withColumn("payload1", F.expr("repeat('A', 500)"))

# Tạo DataFrame 2: Bảng nhỏ hơn một chút, nhưng cũng có id = 1
df2 = spark.range(0, 500000).withColumn(
    "id", F.when(F.rand() < 0.9, 1).otherwise(F.col("id"))
).withColumn("payload2", F.expr("repeat('B', 500)"))

# Thực hiện Sort-Merge Join
# Lượng Cartesian Product của id = 1 sẽ tạo ra (900.000 * 450.000) dòng rác
print("=== Kích nổ Shuffle Spill ===")
df_join = df1.join(df2, "id")
df_join.write.format("noop").mode("overwrite").save()
```

## 4. Forensic Analysis (Phân Tích Pháp Y)

### Bằng chứng 1: Sự lệch pha vật lý của Java Object
Chạy Job trên và mở Spark UI, tab **Stages**. Tìm Stage có chữ `SortMergeJoin`.
Bạn sẽ thấy 2 cột thông số khổng lồ:
- **Spill (Memory):** Có thể lên tới hàng chục GB (Ví dụ: 30GB).
- **Spill (Disk):** Chỉ khoảng vài GB (Ví dụ: 3GB).

> [!CAUTION] Staff-Level Insight: Tại sao có sự lệch pha x10 này?
> Khi dữ liệu nằm trên RAM, nó mang hình hài của các **Object Java**. Một chuỗi string A nhỏ xíu khi được bọc trong class String của Java sẽ mang theo Object Header, Padding... phình to gấp hàng chục lần so với kích thước thật. Cột `Spill (Memory)` phản ánh kích thước béo phì này lềnh bềnh trên RAM.
> 
> Tuy nhiên, khi RAM bị đầy, Tungsten vung đao bóp nghẹt các Object này, Serialize chúng thành mã nhị phân, nén bằng thuật toán (LZ4/Snappy), và nôn xuống đĩa (Spill Disk). Cột `Spill (Disk)` là kích thước đã bị nén chặt. **Tuyệt đối KHÔNG BAO GIỜ cộng hai cột này lại với nhau**.

### Bằng chứng 2: Quả bom nổ chậm trên ổ cứng Local
Vào terminal của Worker Node (hoặc máy local của bạn), tìm thư mục được cấu hình trong `spark.local.dir` (Mặc định thường là `/tmp` của Linux).
Bạn sẽ thấy hàng vạn file `shuffle_*.data` và `shuffle_*.index` phình to liên tục. Nếu ổ `/tmp` của bạn chỉ có 20GB, Job sẽ chết thảm khốc với dòng log: `No space left on device`.

## 5. Mitigation & Takeaways (Bài Học Staff-Level)

1. **OOM không chỉ ở RAM:** Kỹ sư thường mù quáng tăng RAM (`--executor-memory`) khi thấy Job chết. Nhưng nếu Job chết vì Spill Disk làm đầy ổ cứng (Disk OOM), thì tăng RAM thêm 100GB cũng chỉ làm kéo dài cơn hấp hối trước khi đĩa chết hẳn.
2. **Spill không phải là bệnh, nó là tiếng ho báo động:** Việc Spill xuống đĩa thực chất là hành động **Cứu hệ thống** của Spark để tránh việc RAM bị nổ. Đừng cố gắng cấu hình tắt Spill.
3. **Cách khắc phục:** 
   - Giải quyết triệt để vấn đề ở tầng Logic: **Data Skew**. Thực hiện chia để trị bằng Salting (Bài 8.3) để chặt nhỏ Skew Key `1` ra thành `1_A`, `1_B`.
   - Nếu Data quá béo phì do UDF Python sinh ra, hãy dùng Built-in Functions để Tungsten tự quản lý bộ nhớ Unsafe Off-heap thay vì đẻ ra hàng tỷ Object Java béo phì.
   - Theo dõi kỹ lưỡng disk space của thư mục `spark.local.dir`. Trên môi trường Kubernetes (K8s), ổ đĩa cục bộ của Pod thường rất bé, cần phải gắn các Volume lớn (PVC) hoặc dùng Remote Shuffle Service.
