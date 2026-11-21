-- =============================================================================
-- TASK 1.1: KHỞI TẠO CƠ SỞ DỮ LIỆU & CẤP QUYỀN
-- Database: houseprice_db
-- =============================================================================

-- 1. Tạo database houseprice_db nếu chưa có
SELECT 'CREATE DATABASE houseprice_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'houseprice_db')\gexec

-- 2. Cấp toàn quyền trên database cho user airflow
GRANT ALL PRIVILEGES ON DATABASE houseprice_db TO airflow;
