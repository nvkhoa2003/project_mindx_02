-- =============================================================================
-- TASK 5.2: VERIFICATION & AUDIT SCRIPT FOR HOUSEPRICE DATA WAREHOUSE
-- Database: houseprice_db
-- =============================================================================

\connect houseprice_db;

-- -----------------------------------------------------------------------------
-- 1. KIỂM TRA SỐ LƯỢNG BẢN GHI (ROW COUNT AUDIT)
-- -----------------------------------------------------------------------------
SELECT 
    'raw.houseprice_raw' AS table_name, 
    COUNT(*) AS total_rows 
FROM raw.houseprice_raw
UNION ALL
SELECT 
    'dw.fact_house_sales' AS table_name, 
    COUNT(*) AS total_rows 
FROM dw.fact_house_sales
UNION ALL
SELECT 
    'etl.houseprice_reject' AS table_name, 
    COUNT(*) AS total_rows 
FROM etl.houseprice_reject;

-- -----------------------------------------------------------------------------
-- 2. ĐỐI SOÁT TOÁN HỌC DỮ LIỆU: raw - reject = dw
-- -----------------------------------------------------------------------------
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM raw.houseprice_raw) AS raw_cnt,
        (SELECT COUNT(*) FROM etl.houseprice_reject) AS reject_cnt,
        (SELECT COUNT(*) FROM dw.fact_house_sales) AS dw_cnt
)
SELECT 
    raw_cnt,
    reject_cnt,
    dw_cnt,
    (raw_cnt - reject_cnt) AS expected_dw_cnt,
    CASE 
        WHEN (raw_cnt - reject_cnt) = dw_cnt THEN 'PASSED (Match 100%)'
        ELSE 'FAILED (Discrepancy detected)'
    END AS audit_status
FROM counts;

-- -----------------------------------------------------------------------------
-- 3. KIỂM TRA SỐ LƯỢNG BẢN GHI TRÊN CÁC BẢNG DIMENSION (STAR SCHEMA)
-- -----------------------------------------------------------------------------
SELECT 'dw.dim_location' AS dim_table, COUNT(*) AS unique_records FROM dw.dim_location
UNION ALL
SELECT 'dw.dim_building' AS dim_table, COUNT(*) AS unique_records FROM dw.dim_building
UNION ALL
SELECT 'dw.dim_amenities' AS dim_table, COUNT(*) AS unique_records FROM dw.dim_amenities
UNION ALL
SELECT 'dw.dim_sale_contract' AS dim_table, COUNT(*) AS unique_records FROM dw.dim_sale_contract;

-- -----------------------------------------------------------------------------
-- 4. KIỂM TRA TÍNH TOÀN VẸN (INTEGRITY & ORPHAN FK CHECKS)
-- -----------------------------------------------------------------------------
-- 4.1 Kiểm tra giá bán âm hoặc NULL
SELECT 
    COUNT(*) AS invalid_price_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED: Found <=0 or NULL prices' END AS status
FROM dw.fact_house_sales
WHERE sale_price IS NULL OR sale_price <= 0;

-- 4.2 Kiểm tra Foreign Keys mồ côi (Orphan records)
SELECT 
    COUNT(*) AS orphan_fk_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED: Orphan foreign keys found' END AS status
FROM dw.fact_house_sales
WHERE location_id IS NULL 
   OR building_id IS NULL 
   OR amenities_id IS NULL 
   OR contract_id IS NULL;

-- -----------------------------------------------------------------------------
-- 5. THỐNG KÊ KINH DOANH CHÍNH (METRICS SUMMARY)
-- -----------------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_houses_sold,
    TO_CHAR(SUM(sale_price), 'FM$999,999,999,990.00') AS total_sales_volume,
    TO_CHAR(AVG(sale_price), 'FM$999,999,990.00') AS avg_sales_price,
    TO_CHAR(MIN(sale_price), 'FM$999,990.00') AS min_price,
    TO_CHAR(MAX(sale_price), 'FM$999,999,990.00') AS max_price,
    ROUND(AVG(gr_liv_area), 1) AS avg_living_area_sqft,
    ROUND(AVG(lot_area), 1) AS avg_lot_area_sqft
FROM dw.fact_house_sales;

-- -----------------------------------------------------------------------------
-- 6. TOP 5 KHU VỰC CÓ GIÁ NHÀ TRUNG BÌNH CAO NHẤT
-- -----------------------------------------------------------------------------
SELECT 
    l.neighborhood,
    COUNT(f.sale_id) AS total_houses,
    ROUND(AVG(f.sale_price), 2) AS avg_price,
    ROUND(MIN(f.sale_price), 2) AS min_price,
    ROUND(MAX(f.sale_price), 2) AS max_price
FROM dw.fact_house_sales f
JOIN dw.dim_location l ON f.location_id = l.location_id
GROUP BY l.neighborhood
ORDER BY avg_price DESC
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 7. XEM CÁC BẢN GHI BỊ LOẠI VÀ LÝ DO (NẾU CÓ)
-- -----------------------------------------------------------------------------
SELECT 
    reject_id,
    raw_id,
    id,
    reason,
    rejected_at
FROM etl.houseprice_reject
ORDER BY reject_id DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 8. LỊCH SỬ THỰC THI PIPELINE TRÊN BẢNG LOG (CÙNG 1 RUN_ID)
-- -----------------------------------------------------------------------------
SELECT 
    log_id,
    run_id,
    step_name,
    status,
    message,
    created_at
FROM etl.etl_log
ORDER BY created_at DESC
LIMIT 20;
