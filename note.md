17/11: gửii lại đề tài mọi người chọn, 21/11: gửi link sản phầm
23/11 present

# Bước 1: Khởi tạo database Airflow và tạo tài khoản admin (chỉ chạy 1 lần đầu)
docker compose up airflow-init
# Bước 2: Bật toàn bộ các dịch vụ
docker compose up -d

Dọn dẹp & Reset hệ sinh thái
```bash
docker compose down -v
```
*(Lệnh trên sẽ xóa sạch container và volumes nếu bạn muốn chạy lại từ đầu)*