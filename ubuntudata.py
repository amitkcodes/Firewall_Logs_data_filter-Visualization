import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ✅ Database connection details
DB_USER = "postgres"
DB_PASS = "npl@123"   # aapka password
DB_HOST = "172.16.15.145"
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
        if pd.isna(val) or val is None:
            return None
        return int(float(val))
    except Exception:
        return None

# ✅ Helper: safe bigint/float conversion
def safe_bigint(val):
    try:
        if pd.isna(val) or val is None:
            return None
        return int(float(val))
    except Exception:
        return 0

# ✅ Processed files tracker
PROCESSED_LOG = "processed_files.txt"

def load_processed_files():
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG, "r") as f:
            return set(line.strip() for line in f)
    return set()

def mark_as_processed(file_name):
    with open(PROCESSED_LOG, "a") as f:
        f.write(file_name + "\n")

# ✅ Insert CSV into DB (bulk insert with pandas.to_sql)
def insert_csv_to_db(file_path, conn):
    file_name = os.path.basename(file_path)
    print(f"\n[LOADING] {file_name} ...")

    try:
        df = pd.read_csv(file_path)

        # Known columns
        known_cols = [
            "date", "time", "srcip", "srcport", "dstip", "dstport",
            "proto", "action", "service", "srccountry", "dstcountry",
            "policyid", "sessionid", "sentbyte", "rcvdbyte"
        ]

        # Compute extras (JSON for unknown columns)
        unknown_cols = [col for col in df.columns if col not in known_cols]
        if unknown_cols:
            df['extras'] = df.apply(
                lambda row: json.dumps({k: row[k] for k in unknown_cols if pd.notna(row[k])}),
                axis=1
            )
        else:
            df['extras'] = None

        # Drop unknown columns (now in extras)
        df = df[[col for col in known_cols if col in df.columns] + ['extras']]

        # Apply safe conversions
        if 'srcport' in df: df['srcport'] = df['srcport'].apply(safe_int)
        if 'dstport' in df: df['dstport'] = df['dstport'].apply(safe_int)
        if 'proto' in df: df['proto'] = df['proto'].apply(safe_int)
        if 'sessionid' in df: df['sessionid'] = df['sessionid'].apply(safe_bigint)
        if 'sentbyte' in df: df['sentbyte'] = df['sentbyte'].apply(safe_bigint)
        if 'rcvdbyte' in df: df['rcvdbyte'] = df['rcvdbyte'].apply(safe_bigint)

        # Fill missing known columns with None
        for col in known_cols:
            if col not in df:
                df[col] = None

        # Reorder columns to match table (extras last)
        insert_cols = known_cols + ['extras']
        df = df[insert_cols]

        # Bulk insert with chunks to avoid memory issues and allow partial progress
        chunk_size = 10000  # Adjust based on your file size/memory
        total_inserted = 0
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk.to_sql(
                'ntp_logs',
                conn,
                if_exists='append',
                index=False,
                method='multi'  # Faster bulk insert
            )
            total_inserted += len(chunk)
            print(f"   ✅ {total_inserted} rows inserted from {file_name}...")

        print(f"[DONE] {file_name} → Total Inserted: {total_inserted}")
        mark_as_processed(file_name)
        return True

    except Exception as e:
        print(f"   ❌ Error processing {file_name}: {e}")
        return False

# ✅ Main runner
if __name__ == "__main__":
    csv_folder = r"E:\test1\ntpall\filterout4"
    csv_files = sorted([f for f in os.listdir(csv_folder) if f.endswith("_ntp.csv")])

    if not csv_files:
        print("⚠️ No CSV files found in folder!")
    else:
        processed = load_processed_files()
        remaining_files = [f for f in csv_files if f not in processed]

        if not remaining_files:
            print("🎉 All files already processed!")
        else:
            print(f"📂 Found {len(remaining_files)} remaining files to process.")
            with engine.begin() as conn:
                for file in remaining_files:
                    success = insert_csv_to_db(os.path.join(csv_folder, file), conn)
                    if not success:
                        print(f"⚠️ Skipping {file} due to error. You can retry later.")

            print("\n🎉 Processing complete! Check processed_files.txt for logs.")