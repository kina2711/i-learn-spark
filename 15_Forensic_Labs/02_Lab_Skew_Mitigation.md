# 15.2 Lab: Khắc Phục Sự Mất Cân Bằng Dữ Liệu (Data Skew Mitigation)

## 1. Objectives
- [ ] Tái tạo một vụ kẹt xe Data Skew kinh điển do 1 Key quá lớn.
- [ ] Phân tích code bằng mắt thường và hình ảnh vật lý.
- [ ] Áp dụng tuyệt chiêu Salting (Thêm muối) để bẻ gãy khối dữ liệu khổng lồ.

## 2. Sự Cố Kỹ Thuật: Sự Cố Nghẽn Tại Lệnh GroupBy

### 2.1. Mã Nguồn Hiện Tại (Skewed Code)
Công ty có một bảng dữ liệu 10 Tỷ dòng giao dịch của khách hàng (`transactions.parquet`). 
Vấn đề là: Có một khách hàng tên là Shopee chiếm tới **9 Tỷ giao dịch**. 1 Tỷ giao dịch còn lại chia đều cho 10 triệu khách hàng khác.

Kỹ sư chạy lệnh tính Tổng tiền cho từng khách hàng:

```python
# =========================================================================
# THE INITIAL CODE (SUBOPTIMAL CODE)
# =========================================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Skew_Killer").getOrCreate()

df = spark.read.parquet("s3a://data/transactions.parquet")

# Tính tổng tiền theo tên khách hàng. Lệnh này đòi hỏi Shuffle.
df_grouped = df.groupBy("customer_name").agg(F.sum("amount").alias("total"))

df_grouped.write.parquet("s3a://data/results.parquet")
# KẾT QUẢ TẠI SPARK UI:
# 199 Task chạy xong trong 1 phút (Màu xanh).
# 1 Task duy nhất chạy miệt mài 3 tiếng đồng hồ chưa xong (Kẹt cứng).
```

### 2.2. Phân Tích Nguyên Nhân Cốt Lõi (Phân Tích Vật Lý)
Tại sao 1 Task lại chạy mất 3 tiếng?
Hãy nhớ lại Định luật Shuffle (Chương 6): **Những dòng dữ liệu có cùng Key (cùng tên khách hàng) BẮT BUỘC phải được kéo về chung 1 cái Máy để tính toán**.

- Khách hàng A có 100 dòng. Máy số 1 kéo 100 dòng về tính. Mất 1 giây.
- Khách hàng B có 50 dòng. Máy số 2 kéo về tính. Mất 0.5 giây.
- Khách hàng **Shopee có 9 TỶ DÒNG**. Máy số 92 bị xui xẻo nhận nhiệm vụ kéo 9 Tỷ dòng (Nặng 500GB) về xử lý. Máy 92 bị vượt quá công suất, RAM không đủ, phải xả giấy nháp ra đĩa cứng (Spill to Disk). Máy 92 xử lý chậm trễ trong 3 tiếng đồng hồ, trong khi 199 máy khác đang ở trạng thái rảnh rỗi!

### 2.3. Giải Pháp Thủ Công: Thêm Muối (Salting)
Nếu bạn xài Spark 3.x, **vị cứu tinh AQE (Adaptive Query Execution - Bài 8.4)** có thể sẽ tự động chia nhỏ cục Skew này giúp bạn. Nhưng nếu AQE bị tắt, hoặc bạn xài Spark đời cũ, bạn PHẢI tự tay phân chia khối dữ liệu 9 Tỷ dòng đó bằng kỹ thuật **Salting (Thêm Muối rắc lộn xộn)**.

> **[Ý tưởng cốt lõi]**
> Thay vì để tên là Shopee (Làm cho mọi dữ liệu dồn về 1 máy). Ta sẽ cố tình gắn thêm một con số ngẫu nhiên từ 1 đến 10 vào tên. 
> Ví dụ: Shopee_1, Shopee_5, Shopee_9. 
> Nhờ 10 cái tên giả này, Spark sẽ bị lừa và CHIA ĐỀU 9 Tỷ dòng đó cho 10 cái Máy khác nhau cùng làm! Tính xong, ta mới gộp 10 kết quả đó lại làm 1.

```python
# =========================================================================
# THE OPTIMIZED SOLUTION (OPTIMIZED CODE - SALTING TECHNIQUE)
# =========================================================================

# Bước 1: Rắc muối. Gắn thêm 1 số ngẫu nhiên (từ 0 đến 9) vào đuôi cái Key.
# Ví dụ: "Shopee" biến thành "Shopee_4"
df_salted = df.withColumn(
    "salted_key", 
    F.concat(F.col("customer_name"), F.lit("_"), F.floor(F.rand() * 10))
)

# Bước 2: GroupBy theo cái Key ĐÃ RẮC MUỐI (Vòng 1 - Local Aggregation)
# Lúc này, 9 Tỷ dòng của Shopee bị băm làm 10 mảnh, chia cho 10 máy làm.
# Máy 1 tính tổng của Shopee_1. Máy 2 tính tổng của Shopee_2. 
# CHẠY CỰC NHANH VÀ KHÔNG AI BỊ NGHẸN!
df_grouped_step_1 = df_salted.groupBy("salted_key", "customer_name").agg(
    F.sum("amount").alias("partial_total")
)

# Bước 3: Gộp kết quả (Vòng 2 - Global Aggregation)
# Lấy kết quả của 10 máy (Shopee_1 đến Shopee_9) cộng lại thành kết quả cuối cùng.
# Lúc này dữ liệu chỉ còn 10 dòng, gom về 1 máy mất đúng 0.1 giây!
df_final = df_grouped_step_1.groupBy("customer_name").agg(
    F.sum("partial_total").alias("total")
)

df_final.write.parquet("s3a://data/results.parquet")
# KẾT QUẢ: Toàn bộ Job hoàn thành trong 2 phút! Không máy nào bị OOM.
```

## 4. Key takeaways
- **Triệu chứng Skew:** 99% Tasks chạy xong cái vèo, 1% Task kẹt cứng hàng tiếng đồng hồ không nhúc nhích. Bật Spark UI lên thấy cột Max Duration gấp 1.000 lần Median Duration.
- **Bản chất vật lý:** Phép toán Hash Partitioner của Spark quá cứng nhắc. Cứ hễ giống Key là ép vào chung 1 File/1 Máy. 
- **Salting (Giải pháp thủ công):** Kỹ thuật cổ điển nhưng hiệu quả tuyệt đối. Biến 1 Key khổng lồ thành N Key nhỏ bằng cách gắn thêm số Random. Buộc Spark phải chia việc cho nhiều máy làm thay vì bắt 1 máy gánh team.
