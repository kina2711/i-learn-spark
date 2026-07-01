# 7.3 Ác Mộng Của Storage: The Small File Problem

## 1. Objectives
- [ ] Phân tích thảm họa vật lý The Small File Problem qua **Phép ẩn dụ Khuân Đá Cuội vs Đá Tảng**.
- [ ] Nhận diện mối liên kết nguy hiểm giữa Số lượng Partition và Số lượng File sinh ra trên đĩa.
- [ ] Sử dụng Code (hàm `coalesce`) để trị dứt điểm căn bệnh này trước khi lưu trữ.

## 2. Mindmap
```mermaid
mindmap
  root((The Small File Problem))
    (Nguyên nhân)
      (Tính chất 1-1: 1 Partition tạo ra 1 File)
      (Data Skew hoặc Filter lọc mất hết dữ liệu nhưng giữ lại khung Partition)
    (Hậu quả Vật lý)
      (HDFS NameNode bị bóp nghẹt vì phải nhớ địa chỉ hàng triệu file rỗng)
      (Spark mất hàng tiếng đồng hồ chỉ để Lập mục lục)
    (Giải pháp)
      (Dùng lệnh coalesce() để gộp các rổ rỗng lại thành 1 rổ đầy)
```

## 3. Content

### 3.1. Phép Ẩn Dụ: 10.000 Viên Đá Cuội vs 1 Tảng Đá
Trong Hadoop HDFS (hoặc Amazon S3, Azure Blob), hệ thống lưu trữ được thiết kế để chứa **Những Tảng Đá Khổng Lồ (Large Files)**. Nó không sinh ra để chứa cát bụi. 
Tuy nhiên, lập trình viên thường xuyên vô tình dội một trận mưa đá cuội (Hàng triệu file nặng vài Kilobytes) vào hệ thống này. Hiện tượng đó gọi là **The Small File Problem (Vấn nạn File Nhỏ)**.

> **[Ví Dụ Trực Quan: Khuân Đá Xây Nhà]**
> Sếp giao cho bạn khuân 100 Tấn đá về xây nhà. 
> - **Cách làm đúng (Large Files):** Bạn đóng 100 Tấn đó thành 100 Tảng đá khổng lồ (Mỗi tảng 1 Tấn). Máy cẩu của bạn chỉ việc thò càng xuống đúng 100 lần để cẩu lên. Cực nhanh!
> - **Cách làm sai (Small Files):** Bạn đập nát 100 Tấn đá đó ra thành 1.000.000 (1 triệu) viên đá cuội. Máy cẩu của bạn vẫn phải thò càng xuống 1 triệu lần để nhặt từng viên đá cuội lên.
> 
> Hậu quả: Dù tổng khối lượng vẫn là 100 Tấn, nhưng chiếc cẩu của bạn đã cháy động cơ vì phải làm **Thao tác Gắp/Nhả (File Open/Close)** quá nhiều lần. Thời gian làm việc tăng lên gấp 10.000 lần.

### 3.2. Căn Bệnh Vật Lý Của HDFS NameNode
Trong HDFS, có một máy chủ Quản lý Thư viện tên là **NameNode**. NameNode có nhiệm vụ ghi nhớ địa chỉ (Metadata) của TẤT CẢ các file đang nằm ở đâu.

Mỗi một file, dù nặng 1 Terabyte hay chỉ 1 Kilobyte, đều tiêu tốn của NameNode **khoảng 150 Bytes bộ nhớ RAM** để lưu trữ địa chỉ.
Nếu bạn ghi vào ổ cứng 1 triệu file rỗng (hoặc vài Kilobyte), bạn đã cướp đi 150MB RAM của NameNode. Nếu hàng ngàn người dùng trong công ty cùng xả rác file nhỏ như vậy, RAM của NameNode sẽ đầy, và NameNode SẬP (Toàn bộ HDFS của công ty ngưng hoạt động!).

Tại sao Spark lại sinh ra File nhỏ? 
Đó là do **Mối quan hệ 1-1 bất biến giữa Partition và File:** Khi bạn gọi lệnh `df.write.parquet()`, Spark đang có bao nhiêu Partition trong RAM, nó sẽ ghi ra ổ cứng bấy nhiêu File.

### 3.3. Giải Phẫu Bằng Code: Hành Trình Sinh Rác
Hãy xem một lập trình viên vô tình phá hủy NameNode của công ty như thế nào.

```python
# =========================================================================
# [ANTI-PATTERN] TỰ ĐỘNG SINH HÀNG NGÀN FILE RỖNG
# =========================================================================

# Khởi tạo: Đọc dữ liệu thô. Có 2000 Partitions. Mỗi Partition chứa rất nhiều dữ liệu.
df = spark.read.parquet("hdfs://raw_data.parquet")

# Hành động Lọc Rác (Vô tình làm trống rỗng Partitions)
# Lập trình viên lọc đi 99% dữ liệu rác.
# Tình trạng RAM lúc này: 2000 Partitions vẫn CÒN ĐÓ, nhưng 1900 Partitions
# hoàn toàn trống rỗng (Chỉ chứa vài dòng dữ liệu).
df_clean = df.filter(col("is_valid") == True)

# ĐÒN CHÍ MẠNG: GHI THẲNG RA ĐĨA
# Spark răm rắp tuân lệnh. Dù Partition trống rỗng, nó vẫn ghi ra Đĩa.
# Hậu quả: 2000 File Parquet được tạo ra trên HDFS. Trong đó 1900 File nặng 1KB (Rác).
df_clean.write.parquet("hdfs://clean_data.parquet")

# =========================================================================
# [BEST-PRACTICE] DỌN DẸP BẰNG HÀM COALESCE (GỘP ĐÁ CUỘI)
# =========================================================================

# Chữa bệnh: Dùng hàm coalesce() để gộp các Partition lại với nhau.
# Hàm coalesce() ĐỨNG BÊN NÀY BỜ SHUFFLE (Narrow). Nó chỉ đơn giản là túm 100 rổ
# trống rỗng ném chung vào 1 rổ lớn. Không gây tắc nghẽn mạng!
# Bóp 2000 Partitions xuống còn 10 Partitions đẫy đà.
df_compacted = df_clean.coalesce(10)

# Ghi ra đĩa:
# Ổ cứng mỉm cười mãn nguyện vì chỉ phải ghi ra đúng 10 File lớn.
df_compacted.write.parquet("hdfs://compact_data.parquet")
```

## 4. Key takeaways
- **Bản chất File Sinh Ra:** Lệnh `write` của Spark hoạt động theo nguyên tắc mù quáng: Có bao nhiêu cục Partition trong bộ nhớ, sẽ có bấy nhiêu file sinh ra dưới ổ cứng. Dù Partition đó rỗng.
- **Thảm họa Metadata:** Cả NameNode của HDFS lẫn Catalyst Optimizer của Spark đều rất ghét File nhỏ. Bắt chúng lật 1 triệu cái mục lục rỗng tuếch tốn nhiều thời gian hơn cả việc đọc bản thân dữ liệu đó.
- **Vũ khí Coalesce:** Luôn kiểm tra kích thước ước tính của dữ liệu trước khi ghi ra Đĩa. Nếu dữ liệu đã bị gọt đẽo mạnh mẽ bởi lệnh `filter`, BẮT BUỘC phải gọi lệnh `coalesce()` để ép hệ thống gộp các rổ dữ liệu (Partitions) lại thành những rổ đầy, tránh xả rác xuống Data Lake của công ty.
