# Forensic Lab 2: Hacking Memory Eviction - Cuộc Chiến Trành Giành RAM

## 1. Objectives (Mục Tiêu Pháp Y)
- [ ] Quan sát tận mắt hiện tượng **Cache Churn** (Sự đẩy ra/nạp vào liên tục) khi RAM bị giới hạn.
- [ ] Chứng minh ranh giới mềm (Soft boundary) của Unified Memory Manager (UMM).
- [ ] Thấy rõ việc Storage Memory bị đẩy ra (Evict) KHÔNG hề gây OOM, nhưng bóp nghẹt hiệu năng (SLA).

## 2. Incident Scenario (Tình Huống Giả Lập)
Một Junior Data Engineer nhìn vào tab Executors trên Spark UI và hốt hoảng báo động: *Anh ơi, Storage Memory đã báo màu xanh đầy 99%, sắp OOM rồi!*. Cùng lúc đó, Job đang chạy một thuật toán Machine Learning lặp lại 10 lần trên cùng một DataFrame. Thay vì chạy nhanh hơn sau khi Cache, Job lại chạy ngày càng chậm. Bạn cần phải mở nắp capo hệ thống để chứng minh cho cậu bé thấy: Storage đầy không gây OOM, nhưng Eviction (Đá văng) mới là kẻ sát nhân làm chậm Job.

## 3. Lab Setup (The Crime Scene)

Khởi tạo PySpark với lượng RAM siêu nhỏ bé (chỉ 1GB cho Executor) để chúng ta dễ dàng ép hệ thống vào trạng thái đói RAM.

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Khởi tạo Spark với lượng RAM tối thiểu để ép Eviction xảy ra
spark = SparkSession.builder \
    .appName("Forensic_Lab_2_Memory_Eviction") \
    .master("local[2]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .getOrCreate()

# Tạo một DataFrame chứa dữ liệu giả mạo kích thước lớn (Khoảng 800MB)
df = spark.range(0, 100000000).toDF("id")
df_large = df.withColumn("data", F.expr("repeat('A', 100)"))

# Đánh dấu dữ liệu này cần được Cache (Lưu ý: Cache là Lazy!)
df_large.cache()

# Lần Action thứ 1: Ép DataFrame chạy và đổ vào Storage Memory
print("=== Vòng 1: Ép nạp vào Storage ===")
df_large.count()

# Lần Action thứ 2: Chạy một thao tác Sort/Shuffle khổng lồ 
# để Execution Memory bùng nổ, buộc nó vung đao đá văng Storage
print("=== Vòng 2: Kích hoạt Execution cướp RAM ===")
df_large.orderBy(F.rand()).count()
```

## 4. Forensic Analysis (Phân Tích Pháp Y)

### Bằng chứng 1: Hiện tượng Eviction trên tab Storage
Sau vòng 1, bạn mở Spark UI `http://localhost:4040`, chuyển sang tab **Storage**.
- Bảng sẽ hiển thị RDD của `df_large` đang chiếm ví dụ `800 MB` / `900 MB` RAM cho phép. Trạng thái xanh mướt (100% Cached).

Sau vòng 2, nếu bạn F5 lại tab **Storage**:
- Bạn sẽ thấy cột `Fraction Cached` tụt thê thảm từ 100% xuống có thể chỉ còn 10% hoặc 0%.
- Cột **Size in Memory** biến mất một cách bí ẩn.
- **Tại sao?** Vì thao tác `orderBy(F.rand())` (Shuffle Sort) cần một lượng Execution Memory khổng lồ. Unified Memory Manager (UMM) nhận lệnh từ Execution: Dọn ngay chỗ cho tao tính toán. Lập tức toàn bộ dữ liệu 800MB trong Storage bị đấm bay khỏi RAM (Evict). 

### Bằng chứng 2: Ác Mộng Recomputation trong tab SQL / DataFrame
Nếu lúc này bạn chạy lại `df_large.count()` lần thứ 3.
Thay vì mất 0.5s như kỳ vọng của việc đọc Cache, nó lại tốn 10 giây (y hệt như lần chạy đầu tiên).
Hãy nhìn vào Physical Plan hoặc tab SQL, bạn sẽ thấy Spark phải quét lại từ dưới lên trên (Recomputation) chứ không thể đọc từ `InMemoryTableScan`.

> [!CAUTION] Staff-Level Warning: Cache Churn
> Nếu Job của bạn có một vòng lặp (ví dụ K-Means training) và Storage bị Evict ở giữa chừng, vòng lặp tiếp theo sẽ phải **tính toán lại từ đầu**. Hiện tượng này gọi là **Cache Churn**. Thanh RAM luôn xanh mướt báo hiệu đầy, nhưng Cache Hit Ratio lại = 0. Hệ thống không sập (Không OOM), nhưng SLA 1 tiếng sẽ bị kéo dài thành 10 tiếng.

## 5. Mitigation & Takeaways (Bài Học Staff-Level)

1. **Chẩn bệnh đúng cách:** Đừng bao giờ sợ thanh bar Storage Memory hiển thị 99% màu xanh. Hãy sợ khi thanh đó liên tục nhảy nhót lên xuống giữa các Stages, đó là dấu hiệu Eviction đang tàn phá hệ thống.
2. **Kỹ thuật Cứu chữa:** 
   - Nếu dữ liệu sau khi Cache quá to, hãy hạ bậc xuống `StorageLevel.MEMORY_AND_DISK`. Khi Execution đá dữ liệu khỏi RAM, nó sẽ trượt xuống Disk thay vì biến mất vĩnh viễn (Spill). Lần sau lấy lại từ Disk vẫn nhanh hơn là phải tính toán lại (Recompute) từ nguồn ban đầu.
   - Thường xuyên gọi `df.unpersist()` thủ công ngay khi không còn dùng đến biến DataFrame đó nữa để chủ động dọn rác, bảo vệ Execution Memory.
3. **Hiểu rõ UMM:** Trong Spark, Toán tính (Execution) là vua, Dữ liệu tĩnh (Storage) là bề tôi. Vua cần đất, bề tôi phải lập tức hiến dâng. Đừng cố gắng đảo ngược định lý vật lý này.
