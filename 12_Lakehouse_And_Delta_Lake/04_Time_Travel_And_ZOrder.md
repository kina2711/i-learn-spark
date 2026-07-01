# 12.4 Phép Thuật Của Delta: Time Travel & Z-Order

## 1. Objectives
- [ ] Khám phá tính năng cỗ máy thời gian (Time Travel) nhờ cơ chế Nhật Ký.
- [ ] Xử lý dọn rác bằng lệnh `VACUUM`.
- [ ] Tối ưu hóa đa chiều bằng kỹ thuật Z-Order qua **Phép ẩn dụ Sắp Xếp Tủ Sách**.

## 2. Mindmap
```mermaid
mindmap
  root((Phép thuật Delta))
    (Time Travel - Cỗ máy thời gian)
      (Lợi dụng file Parquet cũ chưa bị xóa)
      (Bảo Thủ Kho lật lại trang Sổ ngày hôm qua để đọc)
      (Công dụng: Undo lỗi, truy xuất lịch sử, Machine Learning reproducibility)
    (VACUUM - Dọn rác)
      (Thời gian trôi qua, File cũ quá nhiều gây chật đĩa)
      (Lệnh Vacuum sẽ tiêu hủy vĩnh viễn các File cũ)
    (Z-Order - Sắp xếp đa chiều)
      (Sắp xếp theo 1 chiều (Sort) làm mất tính cục bộ của chiều kia)
      (Z-Curve: Đường zigzag gom các điểm gần nhau của cả 2 trục tọa độ)
```

## 3. Content

### 3.1. Time Travel: Cỗ Máy Thời Gian Của Dữ Liệu
Ở Bài 12.3, khi lệnh Update đổi tên John thành Jack (Tạo File B, gạch bỏ File A).
Có một chi tiết cực kỳ quan trọng: **Ông Thủ Kho Delta CHỈ GẠCH BỎ tư cách của File A, chứ ổng KHÔNG XÓA File A ra khỏi ổ cứng!** 
File A (Chứa chữ John) VẪN CÒN NẰM SỜ SỜ TRONG KHO.

Đây tưởng như là sự lãng phí ổ đĩa, nhưng nó lại đẻ ra một tính năng vượt trội nhất của Delta Lake: **Time Travel (Du hành thời gian)**.

> **[Ví Dụ Trực Quan: Xin Xem Lại Sổ Cũ]**
> Ngày hôm sau, Giám đốc chạy xuống báo: *Thằng nhân viên hôm qua sửa bậy rồi. Tôi muốn xem lại bản báo cáo gốc lúc 8h sáng hôm qua!*
> 
> Nếu là Data Lake cũ (Overwrite đè lên file mất rồi), bạn chỉ có nước xách balo về quê chăn bò.
> Nhưng với Delta Lake, bạn chạy ra bảo Thủ Kho: *Bác lật lại trang Sổ số 1 (Ngày hôm qua), xem lúc đó File nào có hiệu lực thì lôi ra cho sếp cháu xem*.
> Thủ Kho lật Sổ, lấy File A ra. Khôi phục hoàn toàn dữ liệu của quá khứ!

```python
# =========================================================================
# CODE DU HÀNH THỜI GIAN (TIME TRAVEL)
# =========================================================================

# 1. Đọc dữ liệu lúc 8h sáng hôm qua (Bằng Timestamp)
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2026-07-01 08:00:00") \
    .load("s3a://data_lake/customers")

# 2. Hoặc đọc dữ liệu theo Phiên bản Sổ (Version 1)
df_v1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("s3a://data_lake/customers")

# Cách cứu rỗi: Ghi đè phiên bản cũ lên phiên bản hiện tại (UNDO)
df_v1.write.format("delta").mode("overwrite").save("s3a://data_lake/customers")
```

### 3.2. Dọn Rác Bằng Lệnh VACUUM
Du hành thời gian rất vui, nhưng nếu 1 năm trôi qua, bạn Update 10.000 lần. Nghĩa là trong kho đang chứa 9.999 File cũ (Rác). Tiền thuê ổ cứng AWS S3 của bạn sẽ gặp sự cố nghiêm trọng!
Delta cung cấp một Máy hút bụi mang tên **VACUUM**. Khi bạn gọi lệnh này, nó sẽ quét qua kho, **tiêu hủy vĩnh viễn** những file nào đã bị gạch sổ và đã quá hạn (Thường là giữ lại 7 ngày gần nhất). 
*(Chú ý: Đã Vacuum thì KHÔNG THỂ Time Travel về vùng đó được nữa).*

### 3.3. Tối Ưu Đa Chiều Z-Order (Data Skipping)
Ở Bài 7.2, chúng ta đã học về Mục lục Min/Max của Parquet. Nó chỉ hoạt động nếu Dữ liệu được `sort()` (sắp xếp).
Nhưng hàm Sort thông thường chỉ sắp xếp được theo **1 Chiều duy nhất**.

> **[Ví Dụ Trực Quan: Bài Toán Sắp Xếp Tủ Sách Đa Chiều]**
> Bạn có một tủ sách. Bạn muốn tìm sách nhanh theo 2 tiêu chí: Tác Giả (X) và Năm Xuất Bản (Y).
> - Nếu bạn Sort theo Tác Giả (A->Z). Thì các sách của Năm 2020 sẽ bị vứt vương vãi khắp nơi trong tủ.
> - Nếu bạn Sort theo Năm (Cũ->Mới). Thì sách của nhà văn Nam Cao sẽ bị vứt vương vãi khắp nơi trong tủ.
> Không có cách nào dùng Sort 1 chiều mà làm hài lòng cả 2.

**Z-Order (Đường cong Z)** là thuật toán toán học tối ưu sinh ra để giải quyết việc này. 
Nó tưởng tượng dữ liệu là không gian tọa độ OXY. Nó vẽ một đường cong hình chữ Z xuyên qua không gian đó, cố gắng **gom các cuốn sách có cùng Tác giả VÀ Cùng Năm về đứng cạnh nhau cục bộ**. 

Khi bạn chạy lệnh `OPTIMIZE ... ZORDER BY (author, year)`, Delta Lake sẽ sắp xếp lại toàn bộ khối File Parquet theo hình chữ Z. 
Kết quả: Mục lục Min/Max của Parquet giờ đây SẠCH SẼ HOÀN HẢO cho cả 2 cột. Khi truy vấn (Filter) theo Tác Giả HOẶC Năm, Spark đều bỏ qua được 90% số lượng File trên đĩa (Data Skipping) một cách vô cùng hiệu quả.

## 4. Key takeaways
- **Time Travel:** Nhờ sự tồn tại song song của dữ liệu Cũ và Mới trên đĩa cứng, được quản lý bằng Sổ Nhật Ký theo phiên bản, ta có thể dễ dàng Undo, Audit (kiểm toán) và Reproduce lại các thí nghiệm Machine Learning.
- **Bắt buộc phải dọn dẹp:** Delta Lake không tự xóa file cũ. Bạn phải lên lịch chạy lệnh `VACUUM` định kỳ (Ví dụ: 1 tuần 1 lần) để không làm cháy túi công ty.
- **Z-Order Indexing:** Không bao giờ dùng lệnh `sort()` thông thường nếu bạn cần Tỉa dữ liệu (Filter) trên nhiều cột khác nhau. Hãy dùng lệnh `ZORDER` của Delta để gom cụm dữ liệu đa chiều đa không gian, tối đa hóa sức mạnh của Predicate Pushdown.
