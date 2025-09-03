import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ✅ Database connection details
DB_USER = "postgres"
DB_PASS = "npl@123"   # aapka password
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ntp_database"

# URL encode password (agar special chars ho)
DB_PASS_ENC = quote_plus(DB_PASS)

# SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS_ENC}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ✅ Helper: safe integer conversion
def safe_int(val):
    try:
        if pd.isna(val):
            return None
        return int(float(val))
    except Exception:
        return None

# ✅ Helper: safe bigint/float conversion
def safe_bigint(val):
    try:
        if pd.isna(val):
            return None
        return int(float(val))
    except Exception:
        return 0

# ✅ Insert CSV into DB
def insert_csv_to_db(file_path, conn):
    print(f"\n[LOADING] {os.path.basename(file_path)} ...")

    df = pd.read_csv(file_path)

    # Known columns (for hybrid table)
    known_cols = {
        "date", "time", "srcip", "srcport", "dstip", "dstport",
        "proto", "action", "service", "srccountry", "dstcountry",
        "policyid", "sessionid", "sentbyte", "rcvdbyte"
    }

    inserted, skipped = 0, 0

    for idx, row in df.iterrows():
        try:
            row_dict = row.to_dict()

            # Extract known fields
            record = {
                "date": str(row_dict.get("date")) if "date" in row_dict else None,
                "time": str(row_dict.get("time")) if "time" in row_dict else None,
                "srcip": row_dict.get("srcip"),
                "srcport": safe_int(row_dict.get("srcport")),
                "dstip": row_dict.get("dstip"),
                "dstport": safe_int(row_dict.get("dstport")),
                "proto": safe_int(row_dict.get("proto")),
                "action": row_dict.get("action"),
                "service": row_dict.get("service"),
                "srccountry": row_dict.get("srccountry"),
                "dstcountry": row_dict.get("dstcountry"),
                "policyid": str(row_dict.get("policyid")) if "policyid" in row_dict else None,
                "sessionid": safe_bigint(row_dict.get("sessionid")),
                "sentbyte": safe_bigint(row_dict.get("sentbyte")),
                "rcvdbyte": safe_bigint(row_dict.get("rcvdbyte")),
            }

            # Extras = all unknown cols
            extras = {k: v for k, v in row_dict.items() if k not in known_cols and pd.notna(v)}
            record["extras"] = json.dumps(extras) if extras else None

            # ✅ Use bindparam for JSONB
            query = text("""
                INSERT INTO ntp_logs (
                    date, time, srcip, srcport, dstip, dstport, proto, action, service,
                    srccountry, dstcountry, policyid, sessionid, sentbyte, rcvdbyte, extras
                ) VALUES (
                    :date, :time, :srcip, :srcport, :dstip, :dstport, :proto, :action, :service,
                    :srccountry, :dstcountry, :policyid, :sessionid, :sentbyte, :rcvdbyte, CAST(:extras AS JSONB)
                )
            """)

            conn.execute(query, record)
            inserted += 1
            if inserted % 1000 == 0:
                print(f"   ✅ {inserted} rows inserted...")

        except Exception as e:
            skipped += 1
            print(f"   ❌ Row {idx} skipped: {e}")

    print(f"[DONE] {os.path.basename(file_path)} → Inserted: {inserted}, Skipped: {skipped}")

# ✅ Main runner
if __name__ == "__main__":
    csv_folder = r"E:\test1\ntpall\filterout4"
    csv_files = sorted([f for f in os.listdir(csv_folder) if f.endswith("_ntp.csv")])

    if not csv_files:
        print("⚠️ No CSV files found in folder!")
    else:
        with engine.begin() as conn:
            for file in csv_files:
                insert_csv_to_db(os.path.join(csv_folder, file), conn)

        print("\n🎉 All NTP data inserted into PostgreSQL successfully!")
