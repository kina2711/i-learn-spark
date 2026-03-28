## **Chapter 1: What Is Apache Spark?**

## **1. Tóm tắt**

Apache Spark là một **unified computing engine** (công cụ tính toán thống nhất) và một tập thư viện dùng để **xử lý dữ liệu song song** trên các cụm máy tính, hỗ trợ nhiều ngôn ngữ như Python, Java, Scala và R, đồng thời có thể chạy từ laptop đến cluster rất lớn. Chương 1 đi qua ba trục chính: Spark là gì, vì sao thế giới dữ liệu cần Spark, và người mới nên bắt đầu chạy Spark theo những cách nào như local, interactive shell hoặc môi trường cloud.

## **Key takeaway**

- Spark là engine tính toán phân tán, không phải hệ lưu trữ dữ liệu lâu dài.
- Giá trị lớn nhất của Spark là tính thống nhất: một nền tảng cho SQL, machine learning, streaming và các bài toán dữ liệu khác.
- Spark ra đời vì dữ liệu tăng nhanh nhưng phần cứng không còn tăng tốc theo kiểu CPU đơn ngày càng nhanh hơn, nên cần mô hình xử lý song song.
- Spark khác các hệ cũ ở chỗ nó tập trung vào computation trên nhiều storage khác nhau và mở rộng mạnh nhờ hệ thư viện.
- Với người học, Chapter 1 quan trọng nhất ở chỗ hiểu “vì sao có Spark” trước khi học syntax hay tối ưu.

## **2. Explain và Intuition**

Spark được mô tả là một **unified computing engine** và tập thư viện để **xử lý dữ liệu song song** trên cluster, nghĩa là nó vừa **cung cấp engine tính toán**, vừa **cung cấp nhiều thư viện để giải quyết các bài toán dữ liệu** phổ biến. Việc “**unified**” rất quan trọng vì trong thực tế một ứng dụng dữ liệu thường không chỉ làm một việc duy nhất, mà phải kết hợp đọc dữ liệu, query, aggregate, machine learning, thậm chí streaming trong cùng một pipeline.

Để dễ hiểu, bạn hãy xem Spark như bộ não điều phối tính toán cho dữ liệu lớn: dữ liệu có thể nằm ở nhiều nơi, còn Spark mang logic xử lý đến gần dữ liệu và chạy song song trên nhiều máy. Điểm khác biệt quan trọng là Spark không cố trở thành nơi lưu trữ dữ liệu vĩnh viễn, mà tập trung làm tốt lớp compute phía trên các storage system đã có sẵn.

## **3. Breakdown**

### **3.1 Spark là unified computing engine**

- Spark được thiết kế để **hỗ trợ nhiều loại phân tích dữ liệu trên cùng một computing engine và cùng một tập API nhất quán**.
- Sự thống nhất này giúp các tác vụ như SQL, machine learning và streaming có thể được ghép lại trong cùng một ứng dụng thay vì phải nối nhiều hệ thống rời rạc.
- Spark hỗ trợ nhiều ngôn ngữ phổ biến và có thể chạy từ máy cá nhân đến cluster hàng nghìn server.

### **3.2 Triết lý thiết kế: unified, computing engine, libraries**

- **Unified**: Spark muốn trở thành một platform thống nhất cho việc xây dựng big data applications, với API có thể ghép nối và tối ưu xuyên suốt toàn pipeline.
- **Computing engine**: Spark chỉ tập trung vào việc đọc dữ liệu từ storage systems và tính toán trên dữ liệu đó, thay vì tự trở thành một hệ lưu trữ lâu dài.
- **Libraries**: Spark mở rộng sức mạnh thông qua các thư viện chuẩn như Spark SQL, MLlib, Spark Streaming, Structured Streaming và GraphX, cùng rất nhiều thư viện cộng đồng.

### **3.3 Bối cảnh big data và nhu cầu xử lý song song**

- Trước đây, phần mềm thường **tự nhanh hơn khi CPU nhanh hơn**, nên nhiều ứng dụng chỉ được **thiết kế cho một processor**.
- Khoảng từ năm 2005, phần cứng **ngừng tăng tốc mạnh ở một CPU đơn** và chuyển sang **nhiều core chạy song song**, khiến phần mềm muốn nhanh hơn thì phải được viết theo hướng parallelism.
- Trong khi đó, **chi phí lưu trữ và thu thập dữ liệu vẫn tiếp tục giảm**, làm cho việc **có rất nhiều dữ liệu trở thành bình thường** nhưng việc **xử lý nó lại cần các mô hình tính toán phân tán** mới như Spark.

### **3.4 Lịch sử hình thành Spark**

- Spark bắt đầu tại UC Berkeley vào năm 2009 như một research project và được công bố trong bài báo “Spark: Cluster Computing with Working Sets” vào năm sau đó.
- Một động lực lớn cho sự ra đời của Spark là **hạn chế của Hadoop MapReduce**, đặc biệt trong các bài toán nhiều bước hoặc có tính lặp như machine learning, nơi **mỗi vòng lặp trong MapReduce thường phải là một job riêng.**
- Sau giai đoạn đầu, Spark mở rộng từ batch sang interactive data science, SQL, machine learning, streaming và graph processing, rồi được đưa vào Apache Software Foundation năm 2013, ra bản 1.0 năm 2014 và 2.0 năm 2016.

### **3.5 Hiện tại và tương lai của Spark**

- Spark là một trong những hệ thống mã nguồn mở phát triển năng động nhất cho large-scale data processing và tiếp tục mở rộng use case theo thời gian.
- Structured Streaming được nêu như một bước tiến lớn, và Spark được nhắc đến như công cụ đang được dùng ở các công ty công nghệ lớn cũng như các tổ chức khoa học như NASA, CERN và Broad Institute.
- Spark chưa phải một công nghệ “đã xong”, mà là một nền tảng còn tiếp tục phát triển mạnh trong tương lai gần.

### **4. Example**

Hãy tưởng tượng một công ty thương mại điện tử có dữ liệu đơn hàng trong S3, log hành vi người dùng, và nhu cầu vừa làm báo cáo doanh thu vừa xây model dự đoán churn. Trước Spark, họ có thể phải ghép nhiều hệ thống cho batch processing, SQL analytics và machine learning, còn với Spark họ có thể đặt nhiều bước đó trên cùng một engine và cùng một tư duy lập trình.

Một ví dụ pipeline điển hình là: đọc dữ liệu từ storage, query và làm sạch bằng SQL hoặc DataFrame, chạy aggregate, rồi dùng tiếp thư viện ML hoặc streaming nếu cần. Đây chính là ý “unified” mà chương 1 muốn truyền đạt: nhiều workload, một nền tảng.

## **5. Cách chạy Spark**

Để học Spark hiệu quả, bạn nên chạy code theo kiểu interactive để vừa đọc vừa thử nghiệm ngay, thay vì chỉ đọc lý thuyết. Spark hỗ trợ nhiều ngôn ngữ như Python, Java, Scala, R và SQL, nhưng bản thân Spark chạy trên JVM, vì vậy yêu cầu nền tảng tối thiểu để chạy Spark là có Java trên máy; nếu dùng Python thì cần thêm Python, còn nếu dùng R thì cần cài R.

### **5.1. Hai cách bắt đầu**

1. **Chạy Spark trên máy cá nhân**

	- Đây là cách phù hợp nhất khi mới học vì bạn kiểm soát được môi trường, dễ mở terminal và chạy từng lệnh một.
	- Bạn không cần có Hadoop cluster để bắt đầu, vì Spark có thể chạy local mode trên chính laptop của bạn.

2. **Chạy Spark trên cloud**

	- Nếu muốn môi trường notebook sẵn sàng để học nhanh, bạn có thể dùng Databricks Community Edition, là môi trường miễn phí trên web để chạy Spark trực tiếp trong trình duyệt.
	- Cách này tiện ở chỗ không phải tự cài đặt nhiều, đồng thời sách cho biết môi trường này có thể dùng để chạy code và dữ liệu ví dụ của sách.

### **5.2. Chạy local chi tiết**

1. **Chuẩn bị môi trường**

	- Trước hết, hãy kiểm tra máy đã có Java hay chưa, vì Spark chạy trên JVM và đây là điều kiện bắt buộc khi chạy Spark local hoặc trên cluster.
	- Nếu bạn định học bằng PySpark thì cần thêm Python; nếu định học bằng R thì cần cài R tương ứng.

2. **Tải Spark**

	- Vào trang tải chính thức của Apache Spark và chọn bản dựng “Pre-built for Hadoop 2.7 and later”, rồi tải file nén về máy.
	- Sách được viết chủ yếu với Spark 2.2, nên mốc an toàn được gợi ý là dùng Spark 2.2 hoặc mới hơn để bắt đầu.

3. **Giải nén và vào thư mục Spark**

	- Sau khi tải xong, bạn giải nén file tarball rồi chuyển vào thư mục vừa giải nén.
	- Trên hệ Unix, ví dụ điển hình là:
		`bashcd ~/Downloads tar -xf spark-2.2.0-bin-hadoop2.7.tgz cd spark-2.2.0-bin-hadoop2.7.tgz`

4. **Hiểu đúng về thư mục Spark**

	- Khi mở thư mục Spark, bạn sẽ thấy rất nhiều file và thư mục con, nhưng lúc mới học bạn không cần quan tâm hết.
	- Phần quan trọng nhất ban đầu là thư mục `bin`, vì đây là nơi chứa các lệnh để mở các interactive console như PySpark, Scala shell và SQL shell.

5. **Nếu muốn nối với Hadoop cluster**

	- Spark vẫn chạy local mà không cần Hadoop, nhưng nếu bạn muốn laptop kết nối tới một Hadoop cluster thì phải tải đúng package phù hợp với phiên bản Hadoop của cluster đó.
	- Với người mới, sách khuyên nên học local trước rồi mới quan tâm đến chuyện ghép với Hadoop cluster.

### **5.3. Mở interactive shell**

1. PySpark

	- Nếu học bằng Python, cách phổ biến nhất là mở PySpark từ thư mục Spark bằng lệnh sau.
		`bash./bin/pyspark`

	- Sau khi shell khởi động, bạn gõ `spark` để kiểm tra và sẽ thấy đối tượng SparkSession đã có sẵn.
	- Đây là cách vào Spark nhanh nhất cho người mới vì bạn có thể chạy từng dòng lệnh Python tương tác ngay lập tức.

1. Scala shell

	- Nếu học bằng Scala, bạn mở shell bằng lệnh sau.
		`bash./bin/spark-shell`

	- Tương tự PySpark, khi shell chạy xong bạn có thể gõ `spark` để thấy SparkSession đã được tạo sẵn.
	- Vì Spark được viết chủ yếu bằng Scala, đây là môi trường gần với “ngôn ngữ gốc” của Spark nhất.

2. SQL shell

	- Nếu muốn tập trung vào Spark SQL, bạn có thể vào SQL console bằng lệnh sau.
		`bash./bin/spark-sql`

	- Cách này đặc biệt phù hợp khi bạn muốn thử query nhanh theo kiểu analyst thay vì viết code Python hay Scala.

3. Nên bắt đầu bằng shell nào

	- Theo nội dung sách, các điểm vào được ưu tiên nhất là Python, Scala và SQL vì phần lớn ví dụ trong sách xoay quanh ba cách này.
	- Nếu bạn mới học và quen Python, nên bắt đầu bằng `pyspark`; nếu muốn hiểu Spark sát hơn với thiết kế ban đầu, có thể học thêm `spark-shell`.

### **5.4. Chạy trên cloud và dữ liệu ví dụ**

1. Databricks Community Edition

	- Nếu không muốn tự cài local hoặc muốn có trải nghiệm notebook thuận tiện hơn, bạn có thể dùng Databricks Community Edition qua trình duyệt web.
	- Đây là môi trường cloud miễn phí do Databricks cung cấp để học Spark, cho phép chạy Scala, Python, SQL hoặc R trực tiếp trên giao diện web.

2. Lợi ích của cloud notebook

	- Cách học này giúp bạn bỏ qua phần cài đặt phức tạp, tập trung ngay vào việc đọc code, chỉnh sửa code và quan sát kết quả.
	- Nó đặc biệt hữu ích nếu mục tiêu của bạn là học khái niệm và thực hành nhanh thay vì tự dựng môi trường từ đầu.

## **6. Application**

- Với data engineer, Chapter 1 giúp định vị Spark trong modern data stack: Spark là lớp compute nằm trên storage và nối nhiều loại workload.
- Với analyst hoặc data scientist, chương này giúp hiểu vì sao Spark phù hợp cho các bài toán dữ liệu lớn hơn khả năng của một máy và vẫn giữ tư duy làm việc gần với bảng dữ liệu và query.
- Với người mới học, ứng dụng thực tế lớn nhất của Chapter 1 là xây đúng mental model trước khi sang Chapter 2 và các chapter về Structured APIs.