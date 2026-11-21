from __future__ import annotations

import os
import sys
import uuid

import psycopg2

RUN_ID = os.getenv("RUN_ID") or f"RUN_{uuid.uuid4().hex[:8]}"

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env var: {name}")
        return default
    return v.strip()

DB = {
    "host": env("DB_HOST", "postgres"),
    "port": int(env("DB_PORT", "5432")),
    "database": env("DB_NAME", "houseprice_db"),
    "user": env("DB_USER"),
    "password": env("DB_PASS"),
    "connect_timeout": int(env("DB_TIMEOUT", "10")),
}

SQL_LOG = "INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (%s,%s,%s,%s);"

SQL_BUILD_STAGING = r'''
DROP TABLE IF EXISTS etl.houseprice_staging_clean;

CREATE TABLE etl.houseprice_staging_clean (
    house_id       INT PRIMARY KEY,
    neighborhood   VARCHAR(50),
    ms_zoning      VARCHAR(20),
    condition1     VARCHAR(50),
    condition2     VARCHAR(50),
    lot_config     VARCHAR(50),
    land_contour   VARCHAR(20),
    bldg_type      VARCHAR(50),
    house_style    VARCHAR(50),
    year_built     INT,
    year_remod_add INT,
    overall_qual   INT,
    overall_cond   INT,
    foundation     VARCHAR(50),
    roof_style     VARCHAR(50),
    central_air    VARCHAR(5),
    heating_qc     VARCHAR(20),
    kitchen_qual   VARCHAR(20),
    full_bath      INT,
    half_bath      INT,
    bedroom_abv_gr INT,
    fireplaces     INT,
    garage_type    VARCHAR(50),
    garage_cars    INT,
    pool_area      INT,
    pool_qc        VARCHAR(20),
    sale_type      VARCHAR(50),
    sale_condition VARCHAR(50),
    sale_price     NUMERIC(12,2) NOT NULL CHECK (sale_price > 0),
    lot_area       NUMERIC(10,2),
    gr_liv_area    NUMERIC(10,2),
    total_bsmt_sf  NUMERIC(10,2),
    garage_area    NUMERIC(10,2),
    wood_deck_sf   NUMERIC(10,2),
    open_porch_sf  NUMERIC(10,2),
    mo_sold        INT,
    yr_sold        INT
);

WITH parsed AS (
    SELECT
        raw_id,
        CASE
            WHEN id ~ '^[0-9]+$' THEN id::INT
            ELSE NULL
        END AS house_id,
        COALESCE(NULLIF(TRIM(neighborhood), ''), 'Unknown') AS neighborhood,
        COALESCE(NULLIF(TRIM(mszoning), ''), 'RL') AS ms_zoning,
        COALESCE(NULLIF(TRIM(condition1), ''), 'Norm') AS condition1,
        COALESCE(NULLIF(TRIM(condition2), ''), 'Norm') AS condition2,
        COALESCE(NULLIF(TRIM(lotconfig), ''), 'Inside') AS lot_config,
        COALESCE(NULLIF(TRIM(landcontour), ''), 'Lvl') AS land_contour,
        COALESCE(NULLIF(TRIM(bldgtype), ''), '1Fam') AS bldg_type,
        COALESCE(NULLIF(TRIM(housestyle), ''), '1Story') AS house_style,
        CASE WHEN yearbuilt ~ '^[0-9]+$' THEN yearbuilt::INT ELSE 1900 END AS year_built,
        CASE WHEN yearremodadd ~ '^[0-9]+$' THEN yearremodadd::INT ELSE 1900 END AS year_remod_add,
        CASE WHEN overallqual ~ '^[0-9]+$' THEN overallqual::INT ELSE 5 END AS overall_qual,
        CASE WHEN overallcond ~ '^[0-9]+$' THEN overallcond::INT ELSE 5 END AS overall_cond,
        COALESCE(NULLIF(TRIM(foundation), ''), 'PConc') AS foundation,
        COALESCE(NULLIF(TRIM(roofstyle), ''), 'Gable') AS roof_style,
        CASE WHEN UPPER(TRIM(centralair)) = 'Y' THEN 'Y' ELSE 'N' END AS central_air,
        COALESCE(NULLIF(TRIM(heatingqc), ''), 'TA') AS heating_qc,
        COALESCE(NULLIF(TRIM(kitchenqual), ''), 'TA') AS kitchen_qual,
        CASE WHEN fullbath ~ '^[0-9]+$' THEN fullbath::INT ELSE 0 END AS full_bath,
        CASE WHEN halfbath ~ '^[0-9]+$' THEN halfbath::INT ELSE 0 END AS half_bath,
        CASE WHEN bedroomabvgr ~ '^[0-9]+$' THEN bedroomabvgr::INT ELSE 0 END AS bedroom_abv_gr,
        CASE WHEN fireplaces ~ '^[0-9]+$' THEN fireplaces::INT ELSE 0 END AS fireplaces,
        CASE WHEN TRIM(garagetype) IN ('', 'NA') THEN 'None' ELSE COALESCE(TRIM(garagetype), 'None') END AS garage_type,
        CASE WHEN garagecars ~ '^[0-9]+$' THEN garagecars::INT ELSE 0 END AS garage_cars,
        CASE WHEN poolarea ~ '^[0-9]+$' THEN poolarea::INT ELSE 0 END AS pool_area,
        CASE WHEN TRIM(poolqc) IN ('', 'NA') THEN 'None' ELSE COALESCE(TRIM(poolqc), 'None') END AS pool_qc,
        COALESCE(NULLIF(TRIM(saletype), ''), 'WD') AS sale_type,
        COALESCE(NULLIF(TRIM(salecondition), ''), 'Normal') AS sale_condition,
        CASE
            WHEN saleprice ~ '^[0-9]+(\.[0-9]+)?$' THEN saleprice::NUMERIC(12,2)
            ELSE NULL
        END AS sale_price,
        CASE WHEN lotarea ~ '^[0-9]+(\.[0-9]+)?$' THEN lotarea::NUMERIC(10,2) ELSE 0 END AS lot_area,
        CASE WHEN grlivarea ~ '^[0-9]+(\.[0-9]+)?$' THEN grlivarea::NUMERIC(10,2) ELSE 0 END AS gr_liv_area,
        CASE WHEN totalbsmtsf ~ '^[0-9]+(\.[0-9]+)?$' THEN totalbsmtsf::NUMERIC(10,2) ELSE 0 END AS total_bsmt_sf,
        CASE WHEN garagearea ~ '^[0-9]+(\.[0-9]+)?$' THEN garagearea::NUMERIC(10,2) ELSE 0 END AS garage_area,
        CASE WHEN wooddecksf ~ '^[0-9]+(\.[0-9]+)?$' THEN wooddecksf::NUMERIC(10,2) ELSE 0 END AS wood_deck_sf,
        CASE WHEN openporchsf ~ '^[0-9]+(\.[0-9]+)?$' THEN openporchsf::NUMERIC(10,2) ELSE 0 END AS open_porch_sf,
        CASE
            WHEN mosold ~ '^[0-9]+$' AND mosold::INT BETWEEN 1 AND 12 THEN mosold::INT
            ELSE 1
        END AS mo_sold,
        CASE
            WHEN yrsold ~ '^[0-9]+$' AND yrsold::INT >= 1800 THEN yrsold::INT
            ELSE 2000
        END AS yr_sold
    FROM raw.houseprice_raw
),
dedup AS (
    SELECT DISTINCT ON (house_id) *
    FROM parsed
    WHERE house_id IS NOT NULL
    ORDER BY house_id, raw_id DESC
),
cleaned AS (
    SELECT *
    FROM dedup
    WHERE sale_price IS NOT NULL AND sale_price > 0
)
INSERT INTO etl.houseprice_staging_clean
SELECT
    house_id, neighborhood, ms_zoning, condition1, condition2, lot_config, land_contour,
    bldg_type, house_style, year_built, year_remod_add, overall_qual, overall_cond,
    foundation, roof_style, central_air, heating_qc, kitchen_qual, full_bath, half_bath,
    bedroom_abv_gr, fireplaces, garage_type, garage_cars, pool_area, pool_qc,
    sale_type, sale_condition, sale_price, lot_area, gr_liv_area, total_bsmt_sf,
    garage_area, wood_deck_sf, open_porch_sf, mo_sold, yr_sold
FROM cleaned;
'''

SQL_LOAD_DIMENSIONS = r'''
-- 1. Nạp dim_location
TRUNCATE TABLE dw.fact_house_sales CASCADE;
TRUNCATE TABLE dw.dim_location RESTART IDENTITY CASCADE;
TRUNCATE TABLE dw.dim_building RESTART IDENTITY CASCADE;
TRUNCATE TABLE dw.dim_amenities RESTART IDENTITY CASCADE;
TRUNCATE TABLE dw.dim_sale_contract RESTART IDENTITY CASCADE;

INSERT INTO dw.dim_location (neighborhood, ms_zoning, condition1, condition2, lot_config, land_contour)
SELECT DISTINCT neighborhood, ms_zoning, condition1, condition2, lot_config, land_contour
FROM etl.houseprice_staging_clean;

-- 2. Nạp dim_building
INSERT INTO dw.dim_building (bldg_type, house_style, year_built, year_remod_add, overall_qual, overall_cond, foundation, roof_style)
SELECT DISTINCT bldg_type, house_style, year_built, year_remod_add, overall_qual, overall_cond, foundation, roof_style
FROM etl.houseprice_staging_clean;

-- 3. Nạp dim_amenities
INSERT INTO dw.dim_amenities (central_air, heating_qc, kitchen_qual, full_bath, half_bath, bedroom_abv_gr, fireplaces, garage_type, garage_cars, pool_area, pool_qc)
SELECT DISTINCT central_air, heating_qc, kitchen_qual, full_bath, half_bath, bedroom_abv_gr, fireplaces, garage_type, garage_cars, pool_area, pool_qc
FROM etl.houseprice_staging_clean;

-- 4. Nạp dim_sale_contract
INSERT INTO dw.dim_sale_contract (sale_type, sale_condition)
SELECT DISTINCT sale_type, sale_condition
FROM etl.houseprice_staging_clean;
'''

SQL_LOAD_FACT = r'''
INSERT INTO dw.fact_house_sales (
    house_id, location_id, building_id, amenities_id, contract_id,
    sale_price, lot_area, gr_liv_area, total_bsmt_sf, garage_area,
    wood_deck_sf, open_porch_sf, mo_sold, yr_sold
)
SELECT
    s.house_id,
    l.location_id,
    b.building_id,
    a.amenities_id,
    c.contract_id,
    s.sale_price,
    s.lot_area,
    s.gr_liv_area,
    s.total_bsmt_sf,
    s.garage_area,
    s.wood_deck_sf,
    s.open_porch_sf,
    s.mo_sold,
    s.yr_sold
FROM etl.houseprice_staging_clean s
LEFT JOIN dw.dim_location l ON
    l.neighborhood IS NOT DISTINCT FROM s.neighborhood AND
    l.ms_zoning IS NOT DISTINCT FROM s.ms_zoning AND
    l.condition1 IS NOT DISTINCT FROM s.condition1 AND
    l.condition2 IS NOT DISTINCT FROM s.condition2 AND
    l.lot_config IS NOT DISTINCT FROM s.lot_config AND
    l.land_contour IS NOT DISTINCT FROM s.land_contour
LEFT JOIN dw.dim_building b ON
    b.bldg_type IS NOT DISTINCT FROM s.bldg_type AND
    b.house_style IS NOT DISTINCT FROM s.house_style AND
    b.year_built IS NOT DISTINCT FROM s.year_built AND
    b.year_remod_add IS NOT DISTINCT FROM s.year_remod_add AND
    b.overall_qual IS NOT DISTINCT FROM s.overall_qual AND
    b.overall_cond IS NOT DISTINCT FROM s.overall_cond AND
    b.foundation IS NOT DISTINCT FROM s.foundation AND
    b.roof_style IS NOT DISTINCT FROM s.roof_style
LEFT JOIN dw.dim_amenities a ON
    a.central_air IS NOT DISTINCT FROM s.central_air AND
    a.heating_qc IS NOT DISTINCT FROM s.heating_qc AND
    a.kitchen_qual IS NOT DISTINCT FROM s.kitchen_qual AND
    a.full_bath IS NOT DISTINCT FROM s.full_bath AND
    a.half_bath IS NOT DISTINCT FROM s.half_bath AND
    a.bedroom_abv_gr IS NOT DISTINCT FROM s.bedroom_abv_gr AND
    a.fireplaces IS NOT DISTINCT FROM s.fireplaces AND
    a.garage_type IS NOT DISTINCT FROM s.garage_type AND
    a.garage_cars IS NOT DISTINCT FROM s.garage_cars AND
    a.pool_area IS NOT DISTINCT FROM s.pool_area AND
    a.pool_qc IS NOT DISTINCT FROM s.pool_qc
LEFT JOIN dw.dim_sale_contract c ON
    c.sale_type IS NOT DISTINCT FROM s.sale_type AND
    c.sale_condition IS NOT DISTINCT FROM s.sale_condition;
'''

SQL_VALIDATE_DUP = "SELECT house_id, COUNT(*) FROM dw.fact_house_sales GROUP BY house_id HAVING COUNT(*) > 1;"
SQL_COUNT = "SELECT (SELECT COUNT(*) FROM etl.houseprice_staging_clean), (SELECT COUNT(*) FROM dw.fact_house_sales);"
SQL_VALIDATE_PRICE = "SELECT COUNT(*) FROM dw.fact_house_sales WHERE sale_price IS NULL OR sale_price <= 0;"

def log(cur, step: str, status: str, msg: str = ""):
    cur.execute(SQL_LOG, (RUN_ID, step, status, msg))

def main():
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            log(cur, "PIPELINE", "START", "Transform + Load Houseprice to DW")

            log(cur, "TRANSFORM", "START", "Build staging clean table and isolate rejects")
            cur.execute(SQL_BUILD_STAGING)

            # Đổ dữ liệu bị loại vào bảng etl.houseprice_reject
            cur.execute("""
                TRUNCATE TABLE etl.houseprice_reject;
                INSERT INTO etl.houseprice_reject (raw_id, id, reason)
                SELECT r.raw_id, r.id,
                    CASE
                        WHEN r.id IS NULL OR r.id !~ '^[0-9]+$' THEN 'Invalid ID format'
                        WHEN r.saleprice IS NULL OR r.saleprice !~ '^[0-9]+(\.[0-9]+)?$' OR r.saleprice::NUMERIC <= 0 THEN 'Invalid or non-positive sale_price'
                        ELSE 'Duplicate house_id record'
                    END AS reason
                FROM raw.houseprice_raw r
                WHERE NOT EXISTS (
                    SELECT 1 FROM etl.houseprice_staging_clean s 
                    WHERE s.house_id::TEXT = r.id AND s.house_id IS NOT NULL
                );
            """)

            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM raw.houseprice_raw),
                    (SELECT COUNT(*) FROM etl.houseprice_staging_clean),
                    (SELECT COUNT(*) FROM etl.houseprice_reject);
            """)
            raw_cnt, clean_cnt, rejected_cnt = cur.fetchone()
            log(cur, "TRANSFORM", "SUCCESS", f"Staging built: raw={raw_cnt}, clean={clean_cnt}, rejected={rejected_cnt}")

            log(cur, "LOAD_DIMS", "START", "Populate Dimension tables")
            cur.execute(SQL_LOAD_DIMENSIONS)
            log(cur, "LOAD_DIMS", "SUCCESS", "Dimensions populated")

            log(cur, "LOAD_FACT", "START", "Populate Fact house sales table")
            cur.execute(SQL_LOAD_FACT)
            log(cur, "LOAD_FACT", "SUCCESS", "Fact table populated")

            log(cur, "VALIDATE", "START", "Check duplicates and counts")
            cur.execute(SQL_VALIDATE_DUP)
            if cur.fetchall():
                raise RuntimeError("Duplicate house_id detected in dw.fact_house_sales")

            cur.execute(SQL_VALIDATE_PRICE)
            invalid_price_cnt = cur.fetchone()[0]
            if invalid_price_cnt > 0:
                raise RuntimeError(f"Invalid sale_price detected: count={invalid_price_cnt}")

            cur.execute(SQL_COUNT)
            staging_cnt, dw_cnt = cur.fetchone()
            if staging_cnt <= 0:
                raise RuntimeError("staging_cnt=0 (no valid rows after cleaning)")
            if staging_cnt != dw_cnt:
                raise RuntimeError(f"Mismatch between staging ({staging_cnt}) and DW ({dw_cnt})")

            log(cur, "VALIDATE", "SUCCESS", f"staging_cnt={staging_cnt}, dw_cnt={dw_cnt}")
            log(cur, "PIPELINE", "SUCCESS", f"ETL finished successfully with {dw_cnt} records")

        conn.commit()
        print(f"ETL PASSED | RUN_ID={RUN_ID} | records={dw_cnt}")
        sys.exit(0)
    except Exception as e:
        conn.rollback()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                log(cur, "PIPELINE", "FAIL", str(e))
        except Exception:
            pass
        print(f"ETL FAILED | RUN_ID={RUN_ID} | ERROR={e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
