-- =============================================================================
-- TASK 1.2: DDL THIẾT KẾ CƠ SỞ DỮ LIỆU & DATA WAREHOUSE (STAR SCHEMA)
-- Database: houseprice_db
-- =============================================================================

\connect houseprice_db;

-- =============================================================================
-- 1. TẠO CÁC SCHEMA CHÍNH
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS etl;

-- Cấp quyền truy cập Schema cho user airflow
GRANT ALL ON SCHEMA raw TO airflow;
GRANT ALL ON SCHEMA dw TO airflow;
GRANT ALL ON SCHEMA etl TO airflow;

-- =============================================================================
-- 2. TẦNG RAW (STAGING): Chứa toàn bộ 81 cột từ file CSV (Dạng TEXT)
-- =============================================================================
DROP TABLE IF EXISTS raw.houseprice_raw CASCADE;

CREATE TABLE raw.houseprice_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    id TEXT,
    mssubclass TEXT,
    mszoning TEXT,
    lotfrontage TEXT,
    lotarea TEXT,
    street TEXT,
    alley TEXT,
    lotshape TEXT,
    landcontour TEXT,
    utilities TEXT,
    lotconfig TEXT,
    landslope TEXT,
    neighborhood TEXT,
    condition1 TEXT,
    condition2 TEXT,
    bldgtype TEXT,
    housestyle TEXT,
    overallqual TEXT,
    overallcond TEXT,
    yearbuilt TEXT,
    yearremodadd TEXT,
    roofstyle TEXT,
    roofmatl TEXT,
    exterior1st TEXT,
    exterior2nd TEXT,
    masvnrtype TEXT,
    masvnrarea TEXT,
    exterqual TEXT,
    extercond TEXT,
    foundation TEXT,
    bsmtqual TEXT,
    bsmtcond TEXT,
    bsmtexposure TEXT,
    bsmtfintype1 TEXT,
    bsmtfinsf1 TEXT,
    bsmtfintype2 TEXT,
    bsmtfinsf2 TEXT,
    bsmtunfsf TEXT,
    totalbsmtsf TEXT,
    heating TEXT,
    heatingqc TEXT,
    centralair TEXT,
    electrical TEXT,
    "1stflrsf" TEXT,
    "2ndflrsf" TEXT,
    lowqualfinsf TEXT,
    grlivarea TEXT,
    bsmtfullbath TEXT,
    bsmthalfbath TEXT,
    fullbath TEXT,
    halfbath TEXT,
    bedroomabvgr TEXT,
    kitchenabvgr TEXT,
    kitchenqual TEXT,
    totrmsabvgrd TEXT,
    functional TEXT,
    fireplaces TEXT,
    fireplacequ TEXT,
    garagetype TEXT,
    garageyrblt TEXT,
    garagefinish TEXT,
    garagecars TEXT,
    garagearea TEXT,
    garagequal TEXT,
    garagecond TEXT,
    paveddrive TEXT,
    wooddecksf TEXT,
    openporchsf TEXT,
    enclosedporch TEXT,
    "3ssnporch" TEXT,
    screenporch TEXT,
    poolarea TEXT,
    poolqc TEXT,
    fence TEXT,
    miscfeature TEXT,
    miscval TEXT,
    mosold TEXT,
    yrsold TEXT,
    saletype TEXT,
    salecondition TEXT,
    saleprice TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 3. TẦNG DATA WAREHOUSE (DW): Star Schema
-- =============================================================================

-- 3.1 Dimension: Location (Vị trí, Quy hoạch & Địa hình)
DROP TABLE IF EXISTS dw.dim_location CASCADE;
CREATE TABLE dw.dim_location (
    location_id SERIAL PRIMARY KEY,
    neighborhood VARCHAR(50),
    ms_zoning VARCHAR(20),
    condition1 VARCHAR(50),
    condition2 VARCHAR(50),
    lot_config VARCHAR(50),
    land_contour VARCHAR(20)
);

-- 3.2 Dimension: Building (Kết cấu kiến trúc, Năm xây dựng & Chất lượng)
DROP TABLE IF EXISTS dw.dim_building CASCADE;
CREATE TABLE dw.dim_building (
    building_id SERIAL PRIMARY KEY,
    bldg_type VARCHAR(50),
    house_style VARCHAR(50),
    year_built INT,
    year_remod_add INT,
    overall_qual INT,
    overall_cond INT,
    foundation VARCHAR(50),
    roof_style VARCHAR(50)
);

-- 3.3 Dimension: Amenities (Nội thất & Tiện ích sinh hoạt)
DROP TABLE IF EXISTS dw.dim_amenities CASCADE;
CREATE TABLE dw.dim_amenities (
    amenities_id SERIAL PRIMARY KEY,
    central_air VARCHAR(5),
    heating_qc VARCHAR(20),
    kitchen_qual VARCHAR(20),
    full_bath INT,
    half_bath INT,
    bedroom_abv_gr INT,
    fireplaces INT,
    garage_type VARCHAR(50),
    garage_cars INT,
    pool_area INT,
    pool_qc VARCHAR(20)
);

-- 3.4 Dimension: Sale Contract (Hình thức hợp đồng & Điều kiện mua bán)
DROP TABLE IF EXISTS dw.dim_sale_contract CASCADE;
CREATE TABLE dw.dim_sale_contract (
    contract_id SERIAL PRIMARY KEY,
    sale_type VARCHAR(50),
    sale_condition VARCHAR(50)
);

-- 3.5 Fact: House Sales (Bảng Fact trung tâm lưu trữ chỉ số đo lường)
DROP TABLE IF EXISTS dw.fact_house_sales CASCADE;
CREATE TABLE dw.fact_house_sales (
    sale_id SERIAL PRIMARY KEY,
    house_id INT NOT NULL,                  -- Id gốc từ file CSV
    location_id INT REFERENCES dw.dim_location(location_id),
    building_id INT REFERENCES dw.dim_building(building_id),
    amenities_id INT REFERENCES dw.dim_amenities(amenities_id),
    contract_id INT REFERENCES dw.dim_sale_contract(contract_id),
    sale_price NUMERIC(12, 2) NOT NULL CHECK (sale_price > 0),
    lot_area NUMERIC(10, 2),
    gr_liv_area NUMERIC(10, 2),
    total_bsmt_sf NUMERIC(10, 2),
    garage_area NUMERIC(10, 2),
    wood_deck_sf NUMERIC(10, 2),
    open_porch_sf NUMERIC(10, 2),
    mo_sold INT CHECK (mo_sold BETWEEN 1 AND 12),
    yr_sold INT CHECK (yr_sold >= 1800),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo Index tăng tốc truy vấn cho Fact table
CREATE INDEX idx_fact_location ON dw.fact_house_sales(location_id);
CREATE INDEX idx_fact_building ON dw.fact_house_sales(building_id);
CREATE INDEX idx_fact_yr_sold ON dw.fact_house_sales(yr_sold);

-- =============================================================================
-- 4. TẦNG ETL LOGGING: Quản lý vết thực thi Pipeline
-- =============================================================================
DROP TABLE IF EXISTS etl.etl_log CASCADE;
CREATE TABLE etl.etl_log (
    log_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'RUNNING'
    rows_affected INT DEFAULT 0,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 5. PHÂN QUYỀN TOÀN BỘ BẢNG CHO USER AIRFLOW
-- =============================================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl TO airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT ALL ON TABLES TO airflow;


-- =============================================================================
-- 5. PHÂN QUYỀN TOÀN BỘ BẢNG CHO USER AIRFLOW
-- =============================================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl TO airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT ALL ON TABLES TO airflow;