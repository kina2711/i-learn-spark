# 15.4 Lab: Quản Trị Bộ Nhớ Với Watermark (Streaming)

## 1. Objectives
- [ ] Chạy một luồng dữ liệu 24/7 và quan sát sự phình to của State Memory.
- [ ] Áp dụng kỹ thuật Watermark để ra lệnh Khóa Sổ.
- [ ] Giải phẫu cấu trúc Lệnh Window + Watermark chuẩn mực.

## 2. Sự Cố Kỹ Thuật: Hệ Thống Trì Trệ Do Chờ Đợi Một Tin Nhắn Đi Trễ 3 Ngày

### 2.1. Mã Nguồn Hiện Tại (Bad Code)
Công ty yêu cầu tính Báo Cáo Doanh Thu Theo Giờ từ một hệ thống Bán hàng đẩy dữ liệu qua Kafka. Giám đốc muốn báo cáo này phải chạy Real-time 24/7 (Cứ có đơn hàng là báo cáo nhảy số).

Bạn viết một đoạn code Structured Streaming cực kỳ ngắn gọn và cho chạy trên Production:

```python
# =========================================================================
# THE INITIAL CODE (SUBOPTIMAL CODE)
# =========================================================================
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("State_OOM_Killer").getOrCreate()

# Đọc từ Băng chuyền Kafka (Giả lập)
df_stream = spark.readStream.format("kafka").load()
df_sales = df_stream.selectExpr("CAST(value AS STRING)").select(
    F.json_tuple("value", "store_id", "amount", "timestamp").alias("store_id", "amount", "event_time")
)

# Gom nhóm theo Cửa sổ 1 Giờ (Ví dụ: 12h-13h)
df_grouped = df_sales.groupBy(
    F.window("event_time", "1 hour"), 
    "store_id"
).agg(F.sum("amount").alias("revenue"))

# Mở vòng lặp vĩnh cửu
df_grouped.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
    
# NGÀY 1: Chạy mượt mà, tiêu tốn 1GB RAM.
# NGÀY 5: Job bỗng dưng tiêu tốn 5GB RAM.
# NGÀY 30: BÙM! Lỗi OOM. Máy chủ Worker sập nguồn. Spark UI đỏ rực.
```

### 2.2. Phân Tích Nguyên Nhân Cốt Lõi (Phân Tích Vật Lý)
Tại sao RAM lại từ từ phình to như quả bóng bơm hơi cho đến ngày 30?
Chìa khóa nằm ở lệnh `.groupBy(window)`.

Trong Streaming (Dòng chảy vô tận), Spark KHÔNG BIẾT KHI NÀO dữ liệu của Ngày 1 mới thật sự kết thúc (Vì lỡ 1 tháng sau có 1 cái máy quẹt thẻ bị mất mạng nay mới có lại và bắn dữ liệu Ngày 1 lên thì sao?).
Vì Spark không biết, nó phải cắn răng CHỊU ĐỰNG: **Nó phải CẮT một phần RAM của máy chủ để mở sẵn cuốn sổ Báo cáo doanh thu Ngày 1 và giữ cuốn sổ đó KHÔNG BAO GIỜ ĐÓNG LẠI (State Memory).**

Đến ngày 30, Spark đang phải Mở cùng lúc 30 x 24 = 720 cuốn sổ Kế toán (Cho mỗi giờ). Toàn bộ RAM của máy Worker bị lấp đầy bởi những cuốn sổ chờ trực người đi trễ. Quả bóng gặp sự cố nghiêm trọng!

### 2.3. Lệnh Cứu Mạng: Watermark (Mốc Khóa Sổ)
Bạn không thể nạp thêm RAM mãi mãi. Bạn PHẢI RA LUẬT CHẾ TÀI: *Tôi chỉ chờ dữ liệu đi trễ tối đa 2 tiếng. Bất cứ ai đến trễ sau 2 tiếng, tôi khóa sổ và đuổi ra ngoài*.

Đó chính là lệnh `.withWatermark()`. 

```python
# =========================================================================
# THE OPTIMIZED SOLUTION (OPTIMIZED CODE - WATERMARK)
# =========================================================================

# Khai báo mốc chờ TRƯỚC KHI GroupBy
# (Luật: Lấy thời gian xa nhất hệ thống từng nhận được TRỪ ĐI 2 giờ.
# Mọi cuốn sổ nào nằm dưới mốc đó -> XÓA SỔ KHỎI RAM NGAY LẬP TỨC!)
df_safe_grouped = df_sales \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
        F.window("event_time", "1 hour"), 
        "store_id"
    ).agg(F.sum("amount").alias("revenue"))

df_safe_grouped.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "s3a://checkpoints/sales_revenue") \
    .start()

# HẬU QUẢ VẬT LÝ:
# Khi đồng hồ điểm 15:00. Dữ liệu mới nhất Spark nhận được là 15:00.
# Watermark = 15:00 - 2 hours = 13:00.
# Spark lập tức cầm chổi quét SẠCH SÀNH SANH mọi cuốn sổ kế toán (State) 
# nằm từ khoảng 12:00 trở về trước. RAM của Ngày 1 đến Ngày 29 bị xóa trắng!
# RAM LUÔN LUÔN DƯ DẢ! HỆ THỐNG SỐNG TỚI NGÀY TẬN THẾ!
```

## 4. Key takeaways
- **Cái bẫy Aggregation trong Streaming:** Bất cứ khi nào bạn dùng hàm `count`, `sum`, `join` trên một luồng Stream, hãy nhớ rằng dưới nắp capo, Spark đang mở một Bảng Nháp (State) trong RAM. 
- **Với Watermark, ít hơn là sống:** Luôn luôn phải có một lệnh `withWatermark()` đi kèm với cột Thời gian (Event Time). Không có nó, Job Streaming của bạn 100% sẽ chết OOM, vấn đề chỉ là nhanh (Vài ngày) hay chậm (Vài tuần).
- **Tuyệt đối không quên Checkpoint:** Ở code Fix, tôi đã kẹp thêm dòng `.option(checkpointLocation)`. Nếu không có thư mục két sắt này trên S3, máy sập mạng khởi động lại, Spark sẽ đọc lại dữ liệu 30 ngày trước TỪ ĐẦU (Đọc rác, tính đúp doanh thu). Bắt buộc phải có Checkpoint để chống lặp dữ liệu (Exactly-Once).
