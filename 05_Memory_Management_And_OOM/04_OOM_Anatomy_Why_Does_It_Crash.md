# 5.4 Giải Phẫu OOM (Out Of Memory): Tại Sao Hệ Thống Lại Chết?

## 1. Objectives
- [ ] Mổ xẻ 3 loại OOM phổ biến nhất trong Apache Spark qua **Phép ẩn dụ Quả Bóng Bay**.
- [ ] Nhận diện tác nhân `collect()` - Kẻ thù của Driver OOM.
- [ ] Nhận diện hiện tượng Data Skew (Lệch dữ liệu) - Kẻ thù của Executor OOM.

## 2. Mindmap
```mermaid
mindmap
  root((OOM Anatomy))
    (Driver OOM - Máy Chủ Chết)
      (Nguyên nhân: Hàm collect() hút toàn bộ dữ liệu về)
      (Broadcast Join một bảng quá khổng lồ)
    (Executor OOM - Máy Con Chết)
      (Vùng Nháp Execution bị thổi phồng)
      (Lệnh GroupBy gặp Data Skew)
    (YARN/Container OOM)
      (OS giết Spark do dùng quá RAM cấp phép)
```

## 3. Content

### 3.1. Phép Ẩn Dụ: Quả Bóng Bay Căng Tràn
Out Of Memory (OOM) là thông báo lỗi kinh hoàng và phổ biến nhất của Data Engineer. Khi lỗi này hiện lên, máy tính của bạn đã chính thức sập nguồn do RAM bị bơm quá sức chứa vật lý.

> **[Ví Dụ Trực Quan: Thổi Quả Bóng Bay]**
> Thanh RAM của bạn giống như một Quả Bóng Bay có thể tích cố định (ví dụ: 10 Lít). Dữ liệu bạn bơm vào chính là Luồng Khí.
> - **Cơ chế An toàn (Spill):** Bình thường, nếu bạn thổi khí gần đầy quả bóng, Spark sẽ thiết kế một cái Van xả áp suất. Khí thừa sẽ xì qua van đó và chảy xuống Ổ cứng (Quá trình Disk Spill). Quả bóng xẹp lại, an toàn, dù hệ thống chạy chậm lại (vì tốc độ đẩy khí xuống ổ cứng rất chậm).
> - **OOM (Nổ Bóng):** Lỗi OOM chỉ xảy ra khi bạn dùng một cỗ máy nén khí khổng lồ, **BƠM MỘT LƯỢNG KHÍ QUÁ ĐỘT NGỘT** vào quả bóng trong tích tắc (Ví dụ: Thổi 20 Lít khí vào quả bóng 10 Lít chỉ trong 1 giây). Van xả áp suất xả không kịp. Quả bóng NỔ TUNG. Hệ thống chết!

Mọi lỗi OOM đều xuất phát từ việc lập trình viên đã viết một câu lệnh ép Spark nạp dữ liệu một cách quá sức đột ngột. Có 2 đối tượng bị vỡ bóng: Máy Chủ Quản Lý (Driver OOM) và Máy Con (Executor OOM).

### 3.2. Sát Thủ Của Máy Quản Lý: Driver OOM
Máy Driver (Người xà ích) thường được cấp rất ít RAM (Ví dụ: 4GB hoặc 8GB), vì nhiệm vụ của nó chỉ là ngồi cầm Sổ công thức điều phối 100 máy Worker làm việc. Dữ liệu thật nằm trên máy Worker.

Thế nhưng, lập trình viên nghiệp dư thường tung ra Đòn chí mạng mang tên `collect()` hoặc `toPandas()`.

```python
# =========================================================================
# [ANTI-PATTERN] NỔ BÓNG MÁY CHỦ (Driver OOM)
# =========================================================================

# Khởi tạo: Một bảng dữ liệu khổng lồ nặng 500GB nằm rải rác ở 100 máy Worker.
df = spark.table("massive_10_billion_rows")

# ĐÒN CHÍ MẠNG: Lệnh collect()
# Bề mặt: Trả về một List dữ liệu để in ra hoặc duyệt bằng vòng lặp Python.
# Vật lý: Máy Driver (4GB RAM) ra lệnh: "100 máy kia, GỬI TẤT CẢ DỮ LIỆU VỀ ĐÂY CHO TAO!"
# 100 máy đồng loạt bơm 500GB dữ liệu qua đường truyền mạng, tống thẳng vào 4GB RAM của Driver.
data_list = df.collect() 

"""
HẬU QUẢ: Quả bóng Driver NỔ TUNG TRONG TÍCH TẮC.
- JVM ném lỗi: `java.lang.OutOfMemoryError: Java heap space`
- Toàn bộ App Spark bị sập hoàn toàn vì Người Xà Ích đã chết.
"""
```

**Cách Phòng Chống:** KHÔNG BAO GIỜ dùng `collect()` trên tập dữ liệu lớn. Hãy dùng `show(10)`, `take(10)`, hoặc nếu muốn ghi file thì hãy để các máy Worker tự chạy hàm `write.parquet()` trực tiếp xuống đĩa (như Bài 1.1 đã học).

### 3.3. Sát Thủ Của Máy Con: Lệch Dữ Liệu (Data Skew) & Executor OOM
Nếu máy Driver không chết, nhưng hệ thống vẫn báo lỗi OOM ở các Task, thì đó là **Executor OOM**.
Nguyên nhân chủ yếu không phải do bạn bơm quá nhiều dữ liệu, mà do bạn chia phần **KHÔNG ĐỀU**.

> **[Ví Dụ Trực Quan: Chia Phần Không Đều - Data Skew]**
> Có 100 Máy tính (Mỗi máy có quả bóng RAM dung lượng 10GB).
> Có 100 Tỉnh thành cần thống kê dữ liệu dân số (Sử dụng lệnh `groupBy(province)`).
> 
> Spark chia 100 Tỉnh cho 100 Máy.
> - Máy 1 xử lý tỉnh Lai Châu (Có 100,000 dân $\rightarrow$ Chiếm 100MB RAM). Quả bóng xẹp lép, làm xong đi ngủ.
> - Máy 2 xử lý TP.Hồ Chí Minh (Có 10.000.000 dân $\rightarrow$ Dữ liệu đột nhiên phình to thành **15GB**). 
> Cả một khối dữ liệu khổng lồ của TP.HCM ập vào quả bóng 10GB của Máy 2 trong tích tắc (để thực hiện lệnh gom nhóm). Quả bóng Máy 2 NỔ TUNG (Executor OOM).
> 
> Hệ thống Spark phát hiện Máy 2 chết, nó lấy Sổ công thức Lineage (Bài 2.3) ném cho Máy 3 làm lại. Máy 3 lại nhận cục dữ liệu 15GB của TP.HCM. Máy 3 LẠI NỔ TUNG. Hệ thống chết theo dây chuyền!

Hiện tượng một cục dữ liệu (Partition) phình to bất thường so với các cục khác được gọi là **Data Skew (Lệch Dữ Liệu)**. Đây là con Trùm cuối (Final Boss) tàn bạo nhất trong việc tối ưu hóa Big Data.

```python
# =========================================================================
# LỖI OOM DO LỆCH DỮ LIỆU (DATA SKEW) KHI GOM NHÓM
# =========================================================================

# Gom nhóm theo thành phố. (TP.HCM chiếm 80% dữ liệu của cả nước).
# Mọi luồng dữ liệu của TP.HCM hội tụ về đúng 1 Máy tính duy nhất do quy luật của GroupBy.
# Máy tính đó bị thổi phồng RAM và chết (Executor OOM).
df_skewed = df.groupBy("city").count()
```

*(Cách khắc phục Data Skew bằng kỹ thuật Salting sẽ được hướng dẫn chi tiết ở Chương 8 - Tối ưu Joins & AQE).*

## 4. Key takeaways
- **OOM là nổ bóng vật lý:** Nó xảy ra khi lượng dữ liệu đột ngột tràn vào RAM nhanh hơn tốc độ van xả an toàn (Spill) có thể đẩy xuống ổ cứng.
- **Driver OOM (Chết Máy Chủ):** 99% do lỗi lập trình viên dùng lệnh `collect()`, `toPandas()` hoặc Broadcast một bảng dữ liệu quá to. Mạng lưới dồn hết về 1 điểm.
- **Executor OOM (Chết Máy Con):** 99% do hiện tượng **Data Skew**. Một máy tính vô tình gánh cục Partition chứa các từ khóa phổ biến (như TP.HCM, hoặc giá trị Null), khiến RAM máy đó bị quá tải cục bộ, trong khi các máy khác đang ngồi chơi.
