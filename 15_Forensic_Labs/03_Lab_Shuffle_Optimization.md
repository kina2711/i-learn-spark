# 15.3 Lab: Tối Ưu Hóa Quá Trình Shuffle (Broadcast Join)

## 1. Objectives
- [ ] Phơi bày sự tàn phá của Sort Merge Join khi ghép 1 bảng lớn và 1 bảng nhỏ.
- [ ] Soi chiếu DAG (Directed Acyclic Graph) để thấy ranh giới Shuffle sinh ra.
- [ ] Áp dụng kỹ thuật Broadcast Join để loại bỏ hoàn toàn chi phí chuyển phát nhanh qua mạng.

## 2. Sự Cố Kỹ Thuật: Mất 1 Tiếng Chỉ Để Tra Cứu Tên Tỉnh

### 2.1. Mã Nguồn Hiện Tại (Bad Code)
Bạn có một bảng sự kiện truy cập web của 1 tỷ người dùng trên toàn thế giới (`logs.parquet` - Khổng lồ, 500GB).
Mỗi dòng chỉ ghi `country_code` (VD: VN, US).
Giám đốc muốn in ra tên quốc gia đầy đủ. Bạn cần Join bảng sự kiện khổng lồ đó với một bảng tra cứu mã quốc gia (`country_dict.csv` - Rất nhỏ, chỉ có 200 dòng, nặng 10KB).

Bạn viết câu Join kinh điển:

```python
# =========================================================================
# THE INITIAL CODE (SUBOPTIMAL CODE)
# =========================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Shuffle_Killer").getOrCreate()

# Bảng khổng lồ 500GB (1 Tỷ dòng)
df_logs = spark.read.parquet("s3a://data/logs.parquet")

# Bảng tí hon 10KB (200 dòng)
df_countries = spark.read.csv("s3a://data/country_dict.csv", header=True)

# Lệnh Join (Cơn ác mộng vật lý)
# Nếu Spark không tự động tối ưu, nó sẽ dùng Sort Merge Join (Băm cả 2 bảng)
df_joined = df_logs.join(df_countries, "country_code", "left")

df_joined.write.parquet("s3a://data/final_report.parquet")
```

### 2.2. Phân Tích Nguyên Nhân Cốt Lõi (Phân Tích Vật Lý)
Chuyện gì đã xảy ra tại lệnh `.join()` nếu chạy bằng **Sort Merge Join**?
(Hãy nhớ lại Bài 8.1 - Hai người hẹn hò phải bay về chung 1 thành phố).

1. **Shuffle Bảng Nhỏ:** Bảng 200 dòng quốc gia bị chặt làm 200 mảnh nhỏ, phân tán qua Cáp quang đến 200 máy Worker khác nhau. (Cái này không tốn mấy thời gian vì bảng rất nhỏ).
2. **Shuffle Bảng Khổng Lồ:** Bảng 1 Tỷ dòng (500GB) CŨNG BỊ CHẶT RA và NHỒI QUA ĐƯỜNG CÁP QUANG 1 GIGABIT để gửi đến 200 máy Worker.
3. Việc nhét 500GB dữ liệu vào đường truyền cáp quang nội bộ khiến toàn bộ hệ thống mạng của Data Center bị nghẹt thở. Quá trình di chuyển (Shuffle Write / Shuffle Read) tốn 45 phút. 

Chúng ta đã ném 45 phút tuổi thanh xuân qua cửa sổ chỉ để... tra cứu tên của 200 cái quốc gia.

### 2.3. Cứu Rỗi: Phát Tờ Rơi Bằng Broadcast Join
Tại sao phải bắt 1 Tỷ người dân di chuyển (Shuffle bảng lớn), trong khi ta có thể BẮT ÉP cuốn từ điển 10KB di chuyển đến tận nhà từng người?

Đó chính là nguyên lý của **Broadcast Join (Phát loa phóng thanh)** (Bài 8.2). Ta copy cuốn từ điển thành 100 bản sao, ném vào RAM của 100 máy Worker. Sau đó 1 Tỷ người dân (bảng lớn) cứ nằm cố định TẠI CHỖ (Data Locality) trên ổ cứng, và tra cứu cuốn từ điển ngay trong RAM của máy mình.

KẾT QUẢ: 0 BYTE SHUFFLE BẢNG LỚN! (Không một ai phải di chuyển qua mạng).

```python
# =========================================================================
# THE OPTIMIZED SOLUTION (OPTIMIZED CODE - BROADCAST JOIN)
# =========================================================================
import pyspark.sql.functions as F

# Bọc bảng nhỏ bằng hàm broadcast().
# Lệnh này gào vào mặt Spark: 
# "HÃY COPY CUỐN TỪ ĐIỂN NÀY CHO TẤT CẢ CÔNG NHÂN! CẤM SHUFFLE BẢNG LỚN!"
df_joined_fast = df_logs.join(
    F.broadcast(df_countries), 
    "country_code", 
    "left"
)

df_joined_fast.write.parquet("s3a://data/final_report.parquet")
# KẾT QUẢ: 
# Không có Stage Shuffle nào được sinh ra cho df_logs.
# Bảng 500GB được đọc và ghi tại chỗ. Thời gian chạy giảm từ 45 phút xuống còn 3 phút!
```

## 4. Key takeaways
- **Sự Ngu Ngốc Của Mặc Định:** Sort Merge Join là thuật toán an toàn nhất (Không bao giờ OOM), nhưng nó Bắt buộc phải Shuffle cả 2 bảng qua mạng. Áp dụng nó cho 1 bảng 500GB và 1 bảng 10KB là sự thiếu tối ưu về mặt thiết kế hệ thống.
- **Vũ Khí Broadcast:** Mọi Data Engineer khi viết chữ `.join()`, hành động đầu tiên trong não phải là: *Có bảng nào nhỏ hơn 10MB không? Có thì quất hàm `broadcast()` ngay lập tức!*.
- **Giới Hạn OOM:** Đừng nổi điên broadcast một bảng nặng 20GB. Toàn bộ 20GB đó sẽ bị nhét vào RAM của Máy Quản Đốc (Driver) trước khi phát đi cho Công nhân. Bơm 20GB vào Driver 8GB $\rightarrow$ Driver Quá Tải (OOM) $\rightarrow$ Cháy sổ Lineage $\rightarrow$ Job Chết. (Chỉ broadcast bảng < 1GB).
