# Forensic Lab 1: Giải Phẫu Lò Phản Ứng Tungsten WSCG

## 1. Objectives (Mục Tiêu Pháp Y)
- [ ] Chứng minh tận mắt sự khác biệt của Physical Plan khi có và không có Whole-Stage CodeGen (WSCG).
- [ ] Truy xuất mã nguồn Java gốc do Tungsten sinh ra tại Runtime.
- [ ] Mổ xẻ vòng lặp `for` nung chảy các toán tử độc lập (`Scan`, `Filter`, `Project`) thành một khối nguyên tử duy nhất.

## 2. Incident Scenario (Tình Huống Giả Lập)
Một Junior Data Engineer than phiền rằng Job của anh ta chạy rất chậm sau khi thêm một hàm Python UDF để tính toán chiều dài chuỗi. Anh ta không hiểu tại sao một thao tác `length(col)` vốn mất 1 phút lại bị kéo dài thành 10 phút. Nhiệm vụ của Staff Engineer là lôi đoạn mã máy ra để chứng minh Bức tường Virtual Dispatch và sự ngừng hoạt động của WSCG.

## 3. Lab Setup (The Crime Scene)

Khởi tạo PySpark với cấu hình hiển thị Log ở mức `DEBUG` để chộp lấy đoạn mã nguồn Java được sinh ra ngầm.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("Forensic_Lab_1") \
    .master("local[2]") \
    .config("spark.sql.codegen.wholeStage", "true") \
    .getOrCreate()

# Chỉnh Log Level để thấy mã Java sinh ra
spark.sparkContext.setLogLevel("DEBUG")

# Tạo mảng dữ liệu 10 triệu dòng
df = spark.range(1, 10000000).toDF("id")
df = df.withColumn("name", (col("id") * 2).cast("string"))

# Logic chuẩn Production (Dùng Built-in function)
good_df = df.filter(col("id") > 1000).select(length(col("name")).alias("name_len"))

# Logic "Tội phạm" (Dùng Python UDF)
@udf("int")
def bad_length(text):
    return len(text) if text else 0

bad_df = df.filter(col("id") > 1000).select(bad_length(col("name")).alias("name_len"))
```

## 4. Forensic Analysis (Phân Tích Pháp Y)

### Bằng chứng 1: Sự biến mất của Dấu Sao `*` (Tungsten Sign)

Hãy chạy lệnh `explain()` trên cả hai Dataframe:

```python
print("=== GOOD DF EXPLAIN ===")
good_df.explain()

print("=== BAD DF EXPLAIN ===")
bad_df.explain()
```

**Kết quả nhận được:**
```text
=== GOOD DF EXPLAIN ===
== Physical Plan ==
*(1) Project [length(name#2) AS name_len#5]
+- *(1) Filter (id#0L > 1000)
   +- *(1) Project [id#0L, cast((id#0L * 2) as string) AS name#2]
      +- *(1) Range (1, 10000000, step=1, splits=2)
```
Tất cả các Node đều có dấu `*(1)`. Bốn toán tử độc lập (`Range`, `Project`, `Filter`, `Project`) đã bị nung chảy thành một hàm Java nguyên khối duy nhất (Stage 1).

```text
=== BAD DF EXPLAIN ===
== Physical Plan ==
Project [bad_length(name#2) AS name_len#10]
+- *(1) Filter (id#0L > 1000)
   +- *(1) Project [id#0L, cast((id#0L * 2) as string) AS name#2]
      +- *(1) Range (1, 10000000, step=1, splits=2)
```
Toán tử `Project [bad_length]` ở trên cùng **MẤT TÍCH DẤU SAO `*`**. Chuỗi WSCG đã bị bẻ gãy. Spark phải nhảy ra khỏi vòng lặp `for` nung chảy, Serialize dữ liệu, gửi qua Python Process (Context Switch), tính toán, rồi gửi ngược lại JVM. CPU gặp sự cố nghiêm trọng vì I/O IPC.

### Bằng chứng 2: Chộp lấy mã nguồn Java nội tạng

Để thấy sức mạnh của Tungsten trên `good_df`, ta có thể ép Spark in ra đoạn mã Java String bằng hàm `debugCodegen()`:

```python
good_df.queryExecution.debug.codegen()
```

Lục lọi trong đống output khổng lồ, bạn sẽ tìm thấy một vòng lặp `while` hoặc `for` gom toàn bộ logic (Cắt gọn để dễ đọc):

```java
// Bằng chứng Pháp Y: CodeGen Java Source Code
public void generate(Object[] references) {
  // ... (khởi tạo)
  
  // Vòng lặp duy nhất thay thế cho hàng triệu lời gọi hàm Virtual Dispatch
  while (range_batchIdx < range_numElements) {
    long range_rowId = range_batchIdx++;
    
    // 1. Logic của Filter (id > 1000)
    if (range_rowId > 1000L) {
      
      // 2. Logic của Project 1 (Tính name = id * 2)
      long calc_id = range_rowId * 2L;
      UTF8String name_str = String.valueOf(calc_id);
      
      // 3. Logic của Project 2 (Tính length)
      int str_len = name_str.numBytes();
      
      // Đẩy thẳng kết quả vào UnsafeRow (Tungsten Off-heap memory)
      append(str_len);
    }
  }
}
```

> [!TIP] Staff-Level Insight
> Hãy nhìn đoạn mã trên. KHÔNG hề có bất kỳ lệnh gọi hàm `next()` nào. KHÔNG hề tạo Object trung gian (Zero Object Materialization). Mọi dữ liệu trượt thẳng trong L1/L2 Cache của CPU. Janino sẽ compile đoạn chuỗi Java này thành Bytecode, và JIT (Just-In-Time) compiler của JVM sẽ nung nó thành Machine Code đập thẳng vào thanh ghi CPU.

## 5. Mitigation & Takeaways (Bài Học Staff-Level)

1. **Khắc phục sự cố:** Xóa ngay Python UDF. Nếu logic cực kỳ phức tạp không thể dùng Built-in Functions, hãy cân nhắc sử dụng Scala UDF (ít nhất nó không dính Python IPC overhead) hoặc Pandas UDF (Vectorized UDF).
2. **Luật ngầm:** UDF là Điểm Nghẽn bẻ gãy màng bảo vệ WSCG. Tại các hệ thống quy mô lớn, việc sử dụng Python UDF trong Pipeline ETL lõi thường bị Block ngay tại khâu Code Review trừ khi có tài liệu chứng minh không thể làm khác.
3. **Hiểu rõ quyền năng:** Spark không ma thuật sinh ra C++. Nó sinh ra **Mã nguồn Java (Java Source Code)** được tối ưu hóa tột độ để đánh lừa Garbage Collector và tối đa hóa sức mạnh của JVM JIT.
