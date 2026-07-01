# 11.2 Stateful Processing & Watermark: Quản Trị Trạng Thái Dữ Liệu Lịch Sử

## 1. Objectives
- [ ] Phân tích điểm nghẽn kiến trúc của Stateful Processing: Nguồn cơn gây rò rỉ bộ nhớ (OOM) trong luồng Streaming.
- [ ] Xác định cơ chế xử lý độ trễ vật lý (Late Data) và phân tách khái niệm giữa Event Time và Processing Time.
- [ ] Khảo sát nguyên lý hoạt động của Watermark trong việc thiết lập giới hạn loại bỏ dữ liệu (Data Eviction).

## 2. Mindmap
```mermaid
mindmap
  root((Streaming State))
    (Nút Thắt Stateful)
      (Lưu trữ giá trị trung gian từ các lô trước để tích lũy cho lô sau)
      (Nguy cơ phình to của State Store trên bộ nhớ RAM hoặc RocksDB)
      (Hậu quả: Suy thoái hiệu năng và sụp đổ OOM hệ thống)
    (Hiện Tượng Dữ Liệu Trễ - Late Data)
      (Sự chênh lệch giữa Event Time (Thời điểm phát sinh) và Processing Time (Thời điểm nhận))
      (Nguyên nhân: Sự cố mạng, độ trễ băng thông, hoặc thiết bị ngắt kết nối)
    (Cơ Chế Watermark)
      (Thiết lập đường ranh giới thời gian trượt (Sliding boundary))
      (Loại bỏ tự động dữ liệu vượt quá ngưỡng dung sai trễ)
      (Tự động thu hồi tài nguyên State ở các toán tử được hỗ trợ như Window Aggregation)
    (State Store Providers)
      (HDFSBacked: Hiệu năng cao nhưng chia sẻ rủi ro với JVM Heap)
      (RocksDB: Tránh rủi ro OOM JVM, hiệu suất được tối ưu Off-heap)
```

## 3. Content

Mô hình Batch Job mang tính hữu hạn (Hoàn tất và giải phóng tài nguyên). Ngược lại, Stream Job là một tiến trình vận hành liên tục (24/7). Đặc điểm này đặt ra một bài toán kiến trúc phức tạp: **Làm thế nào để duy trì tính toán tích lũy (Ví dụ: Tổng doanh thu lũy kế), khi mỗi chu kỳ Micro-batch chỉ xử lý phân mảnh dữ liệu của 100ms hiện tại?**

### 3.1. Điểm Nghẽn Của Quá Trình Stateful (Lưu Giữ Trạng Thái)
Để thực hiện tính toán cộng dồn, hệ thống bắt buộc phải duy trì kết quả trung gian của lô tính toán trước đó. Spark phân bổ một không gian bộ nhớ chuyên biệt gọi là **State Store**.
Spark hỗ trợ 2 cơ chế Provider chính cho State Store:
1. **HDFSBackedStateStoreProvider (Mặc định):** Lưu trữ tập trạng thái trực tiếp trên không gian JVM Heap Memory của Executor. Cơ chế này đem lại tốc độ truy xuất In-Memory tối đa nhưng chịu rủi ro lớn về OOM và các đợt Garbage Collector (GC Pause) kéo dài.
2. **RocksDBStateStoreProvider:** Lưu trữ trạng thái thông qua hệ quản trị cơ sở dữ liệu nhúng (RocksDB) hoạt động ở không gian Off-heap (C++). Dù đánh đổi bằng độ trễ I/O cục bộ, RocksDB giúp bảo vệ an toàn cho hệ thống JVM, cực kỳ hiệu quả khi khối lượng State tăng trưởng đến quy mô hàng chục Gigabytes.

> [!CAUTION] Cảnh Báo Kiến Trúc: Bùng Nổ Trạng Thái Vô Hạn
> Bất chấp việc sử dụng RAM hay RocksDB, nếu tiến trình Streaming vận hành liên tục mà không có cơ chế thu hồi (Eviction), không gian State Store sẽ tăng trưởng tuyến tính và cuối cùng làm bão hòa hệ thống (OOM hoặc Disk Full). Tính năng Stateful Processing luôn đòi hỏi Kỹ sư thiết kế một vòng đời dọn dẹp (Garbage Collection) rõ ràng cho dữ liệu.

### 3.2. Vấn Đề Dữ Liệu Tới Trễ (Late Data)
Rào cản lớn nhất ngăn hệ thống tự động dọn dẹp State là **Dữ liệu tới trễ (Late Data)**.
Ví dụ: Một giao dịch tài chính phát sinh lúc 12:00 PM (Event Time). Do gián đoạn kết nối, phải đến 14:00 PM dữ liệu này mới được đẩy vào Spark (Processing Time).
Nếu Spark đóng sổ và dọn dẹp không gian State của khung giờ 12:00 PM vào lúc 13:00 PM, giao dịch tới trễ này sẽ bị loại bỏ khỏi tính toán lũy kế, dẫn đến sai lệch kết quả (Data Inconsistency).

### 3.3. Cơ Chế Watermark: Khớp Nối Dung Sai
Để cân bằng giữa việc giải phóng tài nguyên hệ thống và cho phép mức độ trễ của dữ liệu, nền tảng giới thiệu khái niệm **Watermark**.
Watermark là một đường biên thời gian trượt (Sliding bound) được Spark thiết lập, tính toán dựa trên mốc thời gian sự kiện (Event Time) trễ nhất ghi nhận được trừ đi một khoảng dung sai (Tolerance) xác định trước.

**[Cơ Chế Vận Hành Watermark]**
Giả thiết Kỹ sư định nghĩa `Watermark = 2 Giờ`.
1. Hệ thống tiếp nhận sự kiện mới nhất mang nhãn Event Time là 15:00 PM.
2. Spark tự động thiết lập một ranh giới lùi lại 2 tiếng: `15:00 - 2h = 13:00 PM`.
3. **Quy tắc loại trừ:** Bất kỳ sự kiện nào mang nhãn Event Time trước 13:00 PM (Ví dụ 12:59) xâm nhập vào hệ thống lúc này, Spark sẽ tự động **loại bỏ (DROP)** và không tham gia vào vòng đời tính toán.
4. **Giải phóng tài nguyên:** Dựa trên đường cơ sở này, Spark an tâm xóa bỏ hoàn toàn bộ nhớ (State Store) duy trì cho các khung thời gian trước 13:00, bảo vệ hệ thống khỏi sự cố cạn kiệt RAM.

🚨 **Hạn Chế Cấu Trúc:** Cơ chế tự động dọn dẹp của Watermark **CHỈ HOẠT ĐỘNG** kết hợp với các toán tử Stateful được thiết kế xoay quanh trục Event Time (Ví dụ: Window Aggregation, Stream-Stream Join). Đối với các toán tử tùy biến như `mapGroupsWithState` hoặc Aggregation toàn cục thiếu Window, Watermark không thể tự động xử lý. Kỹ sư phải can thiệp trực tiếp vào mã logic để quản lý vòng đời (Sử dụng `StateTimeout`).

**[Code Snippet: Thiết Lập Ranh Giới Watermark]**
```python
# MÔ HÌNH ENTERPRISE: Bắt buộc cấu hình Watermark khi thực thi groupBy theo Time Window
streaming_df \
  .withWatermark("eventTime", "2 hours") \ # Định nghĩa dung sai dữ liệu trễ là 2 tiếng
  .groupBy(
    window("eventTime", "10 minutes"), # Phân rã dữ liệu thành các lô 10 phút
    "city"
  ) \
  .count()
```

## 4. Key takeaways
- **Quản trị rủi ro OOM**: Trong Stateful Streaming, khối lượng trạng thái tăng trưởng tuyến tính là một điểm nghẽn cần được kiểm soát nghiêm ngặt.
- **Sự độc lập của các trục thời gian**: Khung tham chiếu Event Time (thời điểm phát sinh vật lý) mang tính quyết định nghiệp vụ, trong khi Processing Time (thời điểm hệ thống tiếp nhận) phụ thuộc vào băng thông và có khả năng sai lệch cao.
- **Nguyên tắc thỏa hiệp Watermark**: Nó hoạt động như một hợp đồng thiết kế (Design Contract) giữa Kỹ sư và hệ thống. Kỹ sư chấp nhận rủi ro loại bỏ dữ liệu vượt quá khoảng dung sai để đảm bảo tính khả dụng (Availability) của phần cứng.
- **Chuyển tiếp**: Ngoài việc quản trị State và Watermark, tính toàn vẹn của một luồng Streaming còn được định đoạt bởi sự an toàn của Checkpoint và khả năng tương thích khi thay đổi cấu trúc bảng. Những thách thức cốt lõi này sẽ được mổ xẻ ở Bài 11.3 (Checkpoint Internals & Schema Evolution).
