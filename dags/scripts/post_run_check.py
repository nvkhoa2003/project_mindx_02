from __future__ import annotations

import os
import sys
import psycopg2

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
}

def main() -> None:
    try:
        conn = psycopg2.connect(**DB)
        with conn.cursor() as cur:
            # 1. Đếm số dòng bảng Raw và DW
            cur.execute("SELECT COUNT(*) FROM raw.houseprice_raw;")
            raw_cnt = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dw.fact_house_sales;")
            dw_cnt = cur.fetchone()[0]

            # 2. Đếm số lượng Dimension records
            cur.execute("SELECT COUNT(*) FROM dw.dim_location;")
            loc_cnt = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dw.dim_building;")
            bldg_cnt = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dw.dim_amenities;")
            amen_cnt = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dw.dim_sale_contract;")
            contract_cnt = cur.fetchone()[0]

            # 3. Tính toán tổng và trung bình giá bán
            cur.execute("""
                SELECT 
                    COALESCE(SUM(sale_price), 0),
                    COALESCE(AVG(sale_price), 0),
                    COALESCE(MIN(sale_price), 0),
                    COALESCE(MAX(sale_price), 0)
                FROM dw.fact_house_sales;
            """)
            total_sales, avg_price, min_price, max_price = cur.fetchone()

            # 4. Kiểm tra tính toàn vẹn (Integrity Checks)
            # 4.1. Không được có giá bán âm hoặc NULL
            cur.execute("SELECT COUNT(*) FROM dw.fact_house_sales WHERE sale_price IS NULL OR sale_price <= 0;")
            invalid_price_cnt = cur.fetchone()[0]
            if invalid_price_cnt > 0:
                raise ValueError(f"CHECK FAILED: Có {invalid_price_cnt} bản ghi có sale_price <= 0 hoặc NULL!")

            # 4.2. Không được có bản ghi mồ côi (Orphan Foreign Keys)
            cur.execute("""
                SELECT COUNT(*) FROM dw.fact_house_sales 
                WHERE location_id IS NULL 
                   OR building_id IS NULL 
                   OR amenities_id IS NULL 
                   OR contract_id IS NULL;
            """)
            orphan_cnt = cur.fetchone()[0]
            if orphan_cnt > 0:
                raise ValueError(f"CHECK FAILED: Có {orphan_cnt} bản ghi trong Fact table bị thiếu khóa ngoại!")

            # 4.3. Kiểm tra số lượng dòng tối thiểu
            if dw_cnt == 0:
                raise ValueError("CHECK FAILED: Bảng Fact không có dữ liệu (dw_cnt = 0)!")

        conn.close()

        print("==================================================================")
        print("                   DATA QUALITY CHECK REPORT                      ")
        print("==================================================================")
        print(f"CHECK OK | raw_cnt={raw_cnt} | dw_cnt={dw_cnt}")
        print(f"Dimensions: location={loc_cnt} | building={bldg_cnt} | amenities={amen_cnt} | contract={contract_cnt}")
        print(f"Sale Price: Total=${float(total_sales):,.2f} | Avg=${float(avg_price):,.2f} | Min=${float(min_price):,.2f} | Max=${float(max_price):,.2f}")
        print("==================================================================")
        sys.exit(0)

    except Exception as e:
        print(f"CHECK FAILED | ERROR={e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
