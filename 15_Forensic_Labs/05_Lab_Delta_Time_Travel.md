# 15.5 Lab: Quản Trị Phiên Bản Dữ Liệu (Delta Lake Time Travel & Z-Order)

## 1. Objectives
- [ ] Chạy lệnh `UPDATE` trên File Parquet truyền thống và đâm sầm vào vách tường báo lỗi.
- [ ] Sử dụng quyền năng `MERGE INTO` của Delta Lake.
- [ ] Xuyên không về quá khứ để cứu dữ liệu bị xóa nhầm (Time Travel).

## 2. Sự Cố Kỹ Thuật: Cập Nhật 1 Dòng, Tốn 1 Tiếng Đồng Hồ

### 2.1. Nỗi Đau Parquet Truyền Thống (Bad Code)
Công ty lưu 1 Tỷ giao dịch Khách hàng dưới dạng File Parquet trên AWS S3. 
Có 100 khách hàng V.I.P vừa được nâng cấp lên hạng Platinum. Bạn phải thay đổi cột `tier = 'Platinum'` cho 100 người này.

Với nền tảng Parquet cũ, việc cập nhật trực tiếp 1 byte vào File Parquet là **BẤT KHẢ THI VỀ MẶT VẬT LÝ**. Bạn bị ép phải làm trò con bò sau:

```python
# =========================================================================
# THE INITIAL CODE (SUBOPTIMAL CODE)
# =========================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Parquet_Nightmare").getOrCreate()

# Bước 1: KÉO TOÀN BỘ 1 TỶ DÒNG (500GB) TỪ S3 LÊN RAM 
df_old = spark.read.parquet("s3a://lake/customers.parquet")
df_updates = spark.read.csv("s3a://updates/vip_100_users.csv")

# Bước 2: Dùng Left Anti Join để XÓA 100 người cũ ra khỏi bảng 1 Tỷ người.
# Lệnh Join này sẽ kích hoạt SHUFFLE toàn bộ 500GB qua mạng. (Siêu chậm!)
df_keep = df_old.join(df_updates, "id", "left_anti")

# Bước 3: Nối (Union) 100 người mới vào 999.999.900 người còn lại.
df_final = df_keep.unionByName(df_updates)

# Bước 4: Đập bỏ cái File S3 cũ, Ghi Lại Toàn Bộ 500GB xuống S3.
df_final.write.mode("overwrite").parquet("s3a://lake/customers.parquet")

# HẬU QUẢ: 
# Để sửa ĐÚNG 100 DÒNG, bạn phải Shuffle và Overwrite 1 TỶ DÒNG. Mất 1 Tiếng.
# Và nếu lúc đang Ghi đè mà Tắt Cúp Điện? TOÀN BỘ DỮ LIỆU CỦA CÔNG TY XÓA SẠCH! (Mất tính ACID).
```

### 2.2. Vị Cứu Tinh Từ Tương Lai: Delta Lake (Good Code)
May mắn thay, hệ thống của bạn đã được nâng cấp lên định dạng Lakehouse: **Delta Lake**.
Delta mang toàn bộ sức mạnh của Cơ sở dữ liệu RDBMS (Thủ Kho có sổ tay) xuống thẳng Đám Mây S3 giá rẻ. 

Thay vì làm 4 bước thiếu tối ưu ở trên, bạn chỉ cần gõ duy nhất 1 hàm `MERGE`.

```python
# =========================================================================
# THE OPTIMIZED SOLUTION (OPTIMIZED CODE - DELTA MERGE)
# =========================================================================
from delta.tables import DeltaTable

# Thay đổi định dạng "parquet" thành "delta".
# Lúc này Spark không nói chuyện với File Parquet nữa, mà nói chuyện với Thủ Kho Delta.
delta_target = DeltaTable.forPath(spark, "s3a://lake/customers_delta")
df_updates = spark.read.csv("s3a://updates/vip_100_users.csv")

# Phát động đòn đánh Upsert (Merge Into)
delta_target.alias("old").merge(
    source = df_updates.alias("new"),
    condition = "old.id = new.id"
).whenMatchedUpdate(set = { "tier": "new.tier" }) \
 .execute()

# KẾT QUẢ VẬT LÝ:
# Lệnh chạy xong trong 1 Phút! 
# Thủ Kho Delta chỉ âm thầm bóc ra những File Parquet nhỏ chứa 100 người kia, 
# sửa và ghi lại đúng cục đó, sau đó lấy Sổ Nhật Ký Gạch Chéo mấy file cũ đi. 
# 99% File Parquet còn lại NẰM IM KHÔNG BỊ ĐỤNG ĐẾN. 
```

### 2.3. Sự Cố Cháy Nhà: Vô Tình Xóa Nhầm Data (Time Travel)
Sau khi bạn chạy lệnh `MERGE` xong. 
Giám đốc xông vào: *Trời ơi, file `vip_100_users.csv` đó là bản Lỗi! Anh đã cập nhật sai 100 người đó thành Platinum rồi. Làm sao bây giờ? Không có bản Backup (Lưu dự phòng) nào cả!*

Nếu là ngày xưa, cả công ty phải ngồi khóc.
Nhưng bạn đang xài Delta. Bạn cười mỉm. Bạn nhớ lại Bài 12.4: Thủ kho Delta CHỈ GẠCH CHÉO TƯ CÁCH, CHỨ KHÔNG XÓA FILE CŨ.

Bạn lên cỗ máy thời gian quay về quá khứ:

```python
# =========================================================================
# BẺ CONG THỜI GIAN (TIME TRAVEL UNDO)
# =========================================================================

# 1. Trích xuất toàn bộ Lịch sử các đời Sổ Nhật Ký của Delta
delta_target.history().show(truncate=False)
# Bạn sẽ thấy Version 1 là lúc bình thường.
# Version 2 là lúc bạn vừa chạy lệnh MERGE làm hỏng dữ liệu.

# 2. Quay về Version 1 (Cứu sống 100 ông bị sửa sai)
df_savior = spark.read.format("delta").option("versionAsOf", 1).load("s3a://lake/customers_delta")

# 3. Ghi đè Version 1 (Sạch) lên hệ thống hiện tại. (Khôi phục)
df_savior.write.format("delta").mode("overwrite").save("s3a://lake/customers_delta")

# Giám đốc ôm chầm lấy bạn! Bạn được thăng chức lên Staff Data Engineer!
```

*(Lời khuyên Senior: Thời gian không phải là vô hạn. Delta chỉ giữ lại file cũ tối đa 30 ngày. Cứ định kỳ cuối tuần, bạn BẮT BUỘC phải ra lệnh `VACUUM` để ông Thủ Kho vứt hết rác (Các file đã bị gạch chéo quá 7 ngày) đi, để tránh sập túi tiền thuê ổ đĩa S3 của công ty).*

## 4. Lời Kết Của Cuốn Sách I-Learn-Spark
Chúc mừng bạn. Bạn đã đi từ những trang tài liệu định nghĩa RDD khô khan, lặn ngụp trong Mạng lưới, Bộ nhớ, đâm đầu vào OOM, Skew, và cuối cùng dùng Delta Lake để bay vào không gian.

- Kẻ nghiệp dư nghĩ rằng viết xong câu lệnh `df.select().join()` là xong việc. 
- Người Kỹ sư vĩ đại hiểu rằng đằng sau câu lệnh đó là 10.000 con chip CPU đang bốc hỏa, hàng tỷ Byte đang chen lấn trong dây cáp quang, và những vùng giấy nháp (RAM) đang chực chờ phát nổ. 

Khi bạn nhìn đoạn code Spark, bạn không còn thấy chữ. Bạn nhìn thấy Dòng Chảy Vật Lý.
Đó là lúc, bạn thực sự đã Đắc Đạo. Cảm ơn bạn đã tham gia khóa huấn luyện khốc liệt nhất của Enterprise Data Engineering!
