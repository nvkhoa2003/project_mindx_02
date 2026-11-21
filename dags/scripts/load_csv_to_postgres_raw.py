from __future__ import annotations

import os
import sys
import uuid

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    import csv

import psycopg2
from psycopg2.extras import execute_values

RUN_ID = f"RUN_{uuid.uuid4().hex[:8]}"

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

# Resolve CSV path dynamically for container vs host environment
DEFAULT_CSV_PATH = "/opt/airflow/data/Houseprice_raw.csv"
if not os.path.exists(DEFAULT_CSV_PATH):
    for candidate in [
        "/opt/airflow/data/Houseprice.csv",
        "/opt/airflow/dags/data/Houseprice_raw.csv",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "Houseprice_raw.csv")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "Houseprice.csv")),
    ]:
        if os.path.exists(candidate):
            DEFAULT_CSV_PATH = candidate
            break

CSV_PATH = env("CSV_PATH", DEFAULT_CSV_PATH)

SQL_LOG = "INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (%s,%s,%s,%s);"

# Danh sách 81 cột tương ứng bảng raw.houseprice_raw
COLUMNS = [
    "id", "mssubclass", "mszoning", "lotfrontage", "lotarea", "street", "alley", "lotshape",
    "landcontour", "utilities", "lotconfig", "landslope", "neighborhood", "condition1", "condition2",
    "bldgtype", "housestyle", "overallqual", "overallcond", "yearbuilt", "yearremodadd", "roofstyle",
    "roofmatl", "exterior1st", "exterior2nd", "masvnrtype", "masvnrarea", "exterqual", "extercond",
    "foundation", "bsmtqual", "bsmtcond", "bsmtexposure", "bsmtfintype1", "bsmtfinsf1", "bsmtfintype2",
    "bsmtfinsf2", "bsmtunfsf", "totalbsmtsf", "heating", "heatingqc", "centralair", "electrical",
    '"1stflrsf"', '"2ndflrsf"', "lowqualfinsf", "grlivarea", "bsmtfullbath", "bsmthalfbath", "fullbath",
    "halfbath", "bedroomabvgr", "kitchenabvgr", "kitchenqual", "totrmsabvgrd", "functional", "fireplaces",
    "fireplacequ", "garagetype", "garageyrblt", "garagefinish", "garagecars", "garagearea", "garagequal",
    "garagecond", "paveddrive", "wooddecksf", "openporchsf", "enclosedporch", '"3ssnporch"', "screenporch",
    "poolarea", "poolqc", "fence", "miscfeature", "miscval", "mosold", "yrsold", "saletype",
    "salecondition", "saleprice"
]

def main() -> None:
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV file not found at: {CSV_PATH}")

        clean_col_keys = [c.replace('"', '') for c in COLUMNS]

        if HAS_PANDAS:
            df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
            df.columns = [c.lower() for c in df.columns]
            rows = df[clean_col_keys].values.tolist()
            total_rows = len(df)
        else:
            with open(CSV_PATH, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = []
                for r in reader:
                    lower_r = {k.lower(): (v if v is not None else "") for k, v in r.items()}
                    rows.append([lower_r.get(k, "") for k in clean_col_keys])
                total_rows = len(rows)

        with conn.cursor() as cur:
            cur.execute(SQL_LOG, (RUN_ID, "INGEST", "START", f"Loading CSV: {CSV_PATH} rows={total_rows}"))

            cur.execute("TRUNCATE TABLE raw.houseprice_raw;")

            col_names_str = ", ".join(COLUMNS)
            insert_sql = f"INSERT INTO raw.houseprice_raw ({col_names_str}) VALUES %s"

            execute_values(cur, insert_sql, rows, page_size=1000)

            cur.execute(SQL_LOG, (RUN_ID, "INGEST", "SUCCESS", f"Inserted rows={len(rows)}"))

        conn.commit()
        print(f"INGEST PASSED | RUN_ID={RUN_ID} | rows={total_rows}")
        sys.exit(0)
    except Exception as e:
        conn.rollback()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SQL_LOG, (RUN_ID, "INGEST", "FAIL", str(e)))
        except Exception:
            pass
        print(f"INGEST FAILED | RUN_ID={RUN_ID} | ERROR={e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
