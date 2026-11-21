# Houseprice Airflow E2E Project (CSV -> Postgres -> DW)

Pipeline End-to-End thu thập, lưu trữ Staging (Raw), làm sạch và xây dựng Data Warehouse (Star Schema) cho tập dữ liệu **Houseprice (Ames Housing)** điều phối tự động bởi **Apache Airflow**.

---

## 🚀 Quick Start (Hướng dẫn chạy nhanh)

1. **Khởi động Docker Desktop** (đảm bảo Docker ở trạng thái *Running*).
2. **Mở Terminal/PowerShell** tại thư mục dự án: `d:\project_mindx_02`
3. **Khởi tạo Database & Tài khoản Admin Airflow (chỉ chạy lần đầu)**:
   ```bash
   docker compose up airflow-init
   ```
4. **Khởi chạy toàn bộ hệ thống (PostgreSQL, Airflow Webserver, Scheduler)**:
   ```bash
   docker compose up -d
   ```
5. **Truy cập Giao diện Airflow Web UI**:
   - Địa chỉ: [http://localhost:8080](http://localhost:8080)
   - Tài khoản: `admin` / Mật khẩu: `admin`
6. **Kích hoạt Pipeline**:
   - Tìm DAG: `houseprice_e2e_csv_to_dw`
   - Bật công tắc (Unpause) và nhấn nút **Trigger DAG** (nút Play).

---

## 🔍 Kiểm tra Dữ liệu (Verify with DBeaver / pgAdmin)

Kết nối vào PostgreSQL (Container) từ máy host:
- **Host**: `localhost`
- **Port**: `5433` *(đã map 5433 để tránh trùng với Postgres local 5432)*
- **Database**: `houseprice_db`
- **User**: `airflow`
- **Password**: `airflow`

### Các câu lệnh SQL kiểm tra:
```sql
-- 1. Kiểm tra số dòng nạp thô
SELECT COUNT(*) FROM raw.houseprice_raw;

-- 2. Kiểm tra số dòng bảng Fact và các Dimension
SELECT COUNT(*) FROM dw.fact_house_sales;
SELECT COUNT(*) FROM dw.dim_location;
SELECT COUNT(*) FROM dw.dim_building;
SELECT COUNT(*) FROM dw.dim_amenities;
SELECT COUNT(*) FROM dw.dim_sale_contract;

-- 3. Thống kê giá bán nhà
SELECT 
    COUNT(*) AS total_houses,
    ROUND(AVG(sale_price), 2) AS avg_price,
    MIN(sale_price) AS min_price,
    MAX(sale_price) AS max_price
FROM dw.fact_house_sales;

-- 4. Xem nhật ký thực thi Pipeline
SELECT * FROM etl.etl_log ORDER BY created_at DESC LIMIT 20;
```

---

## 🔄 Dọn dẹp & Reset hệ sinh thái
```bash
docker compose down -v
```
*(Lệnh trên sẽ xóa sạch container và volumes nếu bạn muốn chạy lại từ đầu)*
