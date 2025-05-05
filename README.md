# 🚀 Hệ Thống ML Dự Đoán Hành Vi Mua Hàng Của Khách Hàng

Một pipeline MLOps chuyển đổi dữ liệu hành vi thương mại điện tử thành các dự đoán mua hàng thời gian thực. Được xây dựng trên các công nghệ mã nguồn mở hiện đại bao gồm Kafka, Flink, Spark, Ray và MLflow, dự án này minh họa một vòng đời ML hoàn chỉnh từ việc thu nạp dữ liệu đến triển khai mô hình. Hệ thống có tính năng CDC (Change Data Capture - Bắt Dữ liệu Thay đổi) tự động, kho dữ liệu đa tầng, cung cấp đặc trưng (feature serving) thời gian thực và khả năng quan sát (observability) toàn diện.

## 📑 Mục lục

  - [📊 Tập Dữ liệu](https://www.google.com/search?q=%23-t%E1%BA%ADp-d%E1%BB%AF-li%E1%BB%87u)
      - [Cấu trúc tệp](https://www.google.com/search?q=%23c%E1%BA%A5u-tr%C3%BAc-t%E1%BB%87p)
      - [Các loại sự kiện](https://www.google.com/search?q=%23c%C3%A1c-lo%E1%BA%A1i-s%E1%BB%B1-ki%E1%BB%87n)
      - [Mô hình hóa: Dự đoán Hành vi Mua hàng của Khách hàng](https://www.google.com/search?q=%23m%C3%B4-h%C3%ACnh-h%C3%B3a-d%E1%BB%B1-%C4%91o%C3%A1n-h%C3%A0nh-vi-mua-h%C3%A0ng-c%E1%BB%A7a-kh%C3%A1ch-h%C3%A0ng)
  - [🌐 Tổng Quan Kiến Trúc](https://www.google.com/search?q=%23-t%E1%BB%95ng-quan-ki%E1%BA%BFn-tr%C3%BAc)
      - [1. Luồng Dữ liệu (Data Pipeline)](https://www.google.com/search?q=%231-lu%E1%BB%93ng-d%E1%BB%AF-li%E1%BB%87u-data-pipeline)
          - [📤 Nguồn Dữ liệu](https://www.google.com/search?q=%23-ngu%E1%BB%93n-d%E1%BB%AF-li%E1%BB%87u)
          - [✅ Xác thực Lược đồ (Schema Validation)](https://www.google.com/search?q=%23-x%C3%A1c-th%E1%BB%B1c-l%C6%B0%E1%BB%A3c-%C4%91%E1%BB%93-schema-validation)
          - [☁️ Tầng Lưu trữ](https://www.google.com/search?q=%23-t%E1%BA%A7ng-l%C6%B0u-tr%E1%BB%AF)
          - [🛒 Spark Streaming](https://www.google.com/search?q=%23-spark-streaming)
      - [2. Luồng Huấn luyện (Training Pipeline)](https://www.google.com/search?q=%232-lu%E1%BB%93ng-hu%E1%BA%A5n-luy%E1%BB%87n-training-pipeline)
          - [🌟 Huấn luyện Phân tán](https://www.google.com/search?q=%23-hu%E1%BA%A5n-luy%E1%BB%87n-ph%C3%A2n-t%C3%A1n)
          - [📦 Quản lý Mô hình](https://www.google.com/search?q=%23-qu%E1%BA%A3n-l%C3%BD-m%C3%B4-h%C3%ACnh)
      - [3. Luồng Phục vụ (Serving Pipeline)](https://www.google.com/search?q=%233-lu%E1%BB%93ng-ph%E1%BB%A5c-v%E1%BB%A5-serving-pipeline)
          - [⚡ Phục vụ Mô hình (Model Serving)](https://www.google.com/search?q=%23-ph%E1%BB%A5c-v%E1%BB%A5-m%C3%B4-h%C3%ACnh-model-serving)
          - [🔍 Dịch vụ Đặc trưng (Feature Service)](https://www.google.com/search?q=%23-d%E1%BB%8Bch-v%E1%BB%A5-%C4%91%E1%BA%B7c-tr%C6%B0ng-feature-service)
      - [4. Khả năng Quan sát (Observability)](https://www.google.com/search?q=%234-kh%E1%BA%A3-n%C4%83ng-quan-s%C3%A1t-observability)
          - [📡 Chỉ số & Giám sát](https://www.google.com/search?q=%23-ch%E1%BB%89-s%E1%BB%91--gi%C3%A1m-s%C3%A1t)
          - [🔒 Quản lý Truy cập](https://www.google.com/search?q=%23-qu%E1%BA%A3n-l%C3%BD-truy-c%E1%BA%ADp)

## 📊 Tập Dữ liệu

> Dữ liệu Hành vi Thương mại Điện tử từ Cửa hàng Đa danh mục

Tập dữ liệu có thể được tìm thấy [tại đây](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data). Tập dữ liệu này chứa dữ liệu hành vi từ hơn 285 triệu sự kiện người dùng trên một trang web thương mại điện tử đa danh mục lớn.

Dữ liệu kéo dài 7 tháng (từ tháng 10 năm 2019 đến tháng 4 năm 2020) và ghi lại các tương tác giữa người dùng và sản phẩm như lượt xem, thêm/xóa khỏi giỏ hàng và mua hàng. Mỗi sự kiện đại diện cho một mối quan hệ nhiều-nhiều giữa người dùng và sản phẩm.

Tập dữ liệu được thu thập bởi dự án Open CDP, một nền tảng dữ liệu khách hàng mã nguồn mở cho phép theo dõi và phân tích dữ liệu hành vi người dùng.

### Cấu trúc tệp

| Trường         | Mô tả                                                                |
| -------------- | -------------------------------------------------------------------- |
| event\_time     | Dấu thời gian UTC khi sự kiện xảy ra                                  |
| event\_type     | Loại sự kiện tương tác của người dùng                                    |
| product\_id     | Mã định danh duy nhất cho sản phẩm                                     |
| category\_id    | Mã định danh danh mục sản phẩm                                        |
| category\_code  | Phân loại danh mục sản phẩm (khi có sẵn cho các danh mục có ý nghĩa) |
| brand          | Tên thương hiệu (chữ thường, có thể thiếu)                            |
| price          | Giá sản phẩm (số thực - float)                                         |
| user\_id        | Mã định danh người dùng vĩnh viễn                                       |
| user\_session   | ID phiên tạm thời, thay đổi sau thời gian dài người dùng không hoạt động |

### Các loại sự kiện

Tập dữ liệu ghi lại bốn loại tương tác của người dùng:

  - **view**: Người dùng đã xem một sản phẩm
  - **cart**: Người dùng đã thêm một sản phẩm vào giỏ hàng
  - **remove\_from\_cart**: Người dùng đã xóa một sản phẩm khỏi giỏ hàng
  - **purchase**: Người dùng đã mua một sản phẩm

### Mô hình hóa: Dự đoán Hành vi Mua hàng của Khách hàng

Nhiệm vụ mô hình hóa cốt lõi là dự đoán liệu người dùng có mua một sản phẩm tại thời điểm họ thêm nó vào giỏ hàng hay không.

#### Kỹ thuật Đặc trưng (Feature Engineering)

Chúng tôi biến đổi dữ liệu sự kiện thô thành các đặc trưng có ý nghĩa cho mô hình học máy của mình. Phân tích tập trung đặc biệt vào các sự kiện thêm vào giỏ hàng và kết quả tiếp theo của chúng.

Các đặc trưng được thiết kế chính bao gồm:

| Đặc trưng            | Mô tả                                                           |
| -------------------- | --------------------------------------------------------------- |
| category\_code\_level1 | Danh mục sản phẩm chính                                         |
| category\_code\_level2 | Danh mục phụ của sản phẩm                                       |
| event\_weekday        | Ngày trong tuần khi sự kiện thêm vào giỏ hàng xảy ra             |
| activity\_count       | Tổng số hoạt động của người dùng trong phiên hiện tại            |
| price                | Giá gốc của sản phẩm                                             |
| brand                | Tên thương hiệu sản phẩm                                        |
| is\_purchased         | Biến mục tiêu: liệu mặt hàng trong giỏ hàng cuối cùng có được mua không |

Bạn có thể tải tập dữ liệu về và đặt nó vào thư mục `data`.

## 🌐 Tổng Quan Kiến Trúc

Hệ thống bao gồm bốn thành phần chính—**Dữ liệu (Data)**, **Huấn luyện (Training)**, **Phục vụ (Serving)**, và **Khả năng Quan sát (Observability)**—cùng với một **Môi trường Phát triển (Dev Environment)** và một **Kho Mô hình (Model Registry)**.

### 1\. Luồng Dữ liệu (Data Pipeline)

#### 📤 Nguồn Dữ liệu

  - **Kafka Producer**: Liên tục phát các sự kiện hành vi người dùng đến topic `tracking.raw_user_behavior`
  - **Dịch vụ CDC**: Sử dụng Debezium để bắt các thay đổi trong PostgreSQL, truyền dữ liệu đến `tracking_postgres_cdc.public.events`

#### ✅ Xác thực Lược đồ (Schema Validation)

  - Xác thực các sự kiện đến từ cả hai nguồn
  - Định tuyến các sự kiện đến:
      - `tracking.user_behavior.validated` cho các sự kiện hợp lệ
      - `tracking.user_behavior.invalid` cho các vi phạm lược đồ
  - Xử lý khoảng 10 nghìn sự kiện/giây
  - Cảnh báo các sự kiện không hợp lệ đến Elasticsearch

#### ☁️ Tầng Lưu trữ

  - **Data Lake (MinIO)**:
      - Lưu trữ Ngoài (External Storage)
      - Lưu trữ dữ liệu trong các bucket được phân vùng theo thời gian (năm/tháng/ngày/giờ)
      - Hỗ trợ điểm kiểm tra (checkpointing) cho khả năng phục hồi của pipeline
  - **Data Warehouse (PostgreSQL)**:
      - Tổ chức theo các lớp bronze → silver → gold
      - Chứa các bảng dimension/fact cho mục đích phân tích
  - **Offline Store (PostgreSQL)**:
      - Được sử dụng để huấn luyện và cung cấp đặc trưng theo lô (batch feature serving)
      - Được cụ thể hóa (materialized) định kỳ vào online store
  - **Online Store (Redis)**:
      - Cung cấp đặc trưng với độ trễ thấp
      - Được cập nhật thông qua luồng xử lý streaming
      - Được cung cấp thông qua API Truy xuất Đặc trưng (Feature Retrieval API)

#### 🛒 Spark Streaming

  - Biến đổi các sự kiện đã được xác thực thành các đặc trưng ML
  - Tập trung vào các chỉ số dựa trên phiên và hành vi mua hàng
  - Ghi đồng thời vào cả online/offline stores

### 2\. Luồng Huấn luyện (Training Pipeline)

#### 🌟 Huấn luyện Phân tán

  - **Cụm Ray (Ray Cluster)**:
      - Xử lý tinh chỉnh siêu tham số phân tán thông qua Ray Tune
      - Thực hiện huấn luyện mô hình cuối cùng
      - Tích hợp với MLflow để theo dõi thử nghiệm

#### 📦 Quản lý Mô hình

  - **MLflow + MinIO + PostgreSQL**:
      - Theo dõi các thử nghiệm, tham số và chỉ số
      - Quản lý phiên bản các tạo tác mô hình (model artifacts)
      - Cung cấp giao diện người dùng kho mô hình (model registry UI) tại `localhost:5001`

### 3\. Luồng Phục vụ (Serving Pipeline)

#### ⚡ Phục vụ Mô hình (Model Serving)

  - **Ray Serve**:
      - Tải mô hình từ kho MLflow
      - Tự động mở rộng quy mô theo chiều ngang để đáp ứng thông lượng cao
      - Cung cấp REST API cho các dự đoán
  - **Dịch vụ Đặc trưng (Feature Service)**:
      - Endpoint FastAPI để truy xuất đặc trưng
      - Tích hợp với Redis cho các đặc trưng thời gian thực

### 4\. Khả năng Quan sát (Observability)

#### 📡 Chỉ số & Giám sát

  - **SigNoz**:
      - Thu thập dữ liệu OpenTelemetry
      - Cung cấp giám sát cấp dịch vụ
      - Truy cập tại `localhost:3301`
  - **Ray Dashboard**:
      - Giám sát các công việc huấn luyện/phục vụ
      - Khả dụng tại `localhost:8265`
  - **Prometheus + Grafana**:
      - Theo dõi các chỉ số của cụm Ray
      - Trực quan hóa hiệu năng hệ thống
      - Truy cập tại `localhost:3009`
  - **Superset**:
      - Trực quan hóa dữ liệu trong Data Warehouse
      - Truy cập tại `localhost:8089`
  - **Elasticsearch**:
      - Cảnh báo các sự kiện không hợp lệ

#### 🔒 Quản lý Truy cập

  - **NGINX Proxy Manager**:
      - Proxy ngược (Reverse proxy) cho tất cả các dịch vụ
      - Chấm dứt SSL/TLS (SSL/TLS termination)
      - Kiểm soát truy cập và định tuyến

Kiến trúc ưu tiên độ tin cậy, khả năng mở rộng và khả năng quan sát trong khi vẫn duy trì sự tách biệt rõ ràng các mối quan tâm giữa các giai đoạn của pipeline. Mỗi thành phần được đóng gói trong container và có thể được triển khai độc lập bằng Docker Compose.

-----
