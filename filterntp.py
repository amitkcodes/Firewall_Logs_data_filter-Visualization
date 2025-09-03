import os
import re
import pandas as pd

INPUT_FOLDER = r"E:\test1\outputall\dataout1"     # input CSV folder
OUTPUT_FOLDER = r"E:\test1\ntpall\filterout1" # filtered CSV folder
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def filter_ntp_large(csv_file, file_index, chunksize=50000):
    input_path = os.path.join(INPUT_FOLDER, csv_file)

    # Output file ka naam serial wise: file_1_ntp.csv, file_2_ntp.csv ...
    output_filename = f"filedata1_{file_index}_ntp.csv"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)

    print(f"[PROCESSING] {csv_file} -> {output_filename}")

    first_chunk = True
    try:
        for chunk in pd.read_csv(input_path, chunksize=chunksize, low_memory=False, on_bad_lines="skip"):
            # normalize column names
            chunk.columns = [c.lower().strip() for c in chunk.columns]

            # Filter by port=123 (if any port column exists)
            port_cols = [c for c in chunk.columns if "port" in c]
            if port_cols:
                for col in port_cols:
                    try:
                        chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
                    except:
                        pass
                chunk = chunk[(chunk[port_cols] == 123).any(axis=1)]

            # Filter by service == "NTP"
            if "service" in chunk.columns:
                chunk = chunk[chunk["service"].astype(str).str.upper() == "NTP"]

            # Save filtered data
            if not chunk.empty:
                chunk.to_csv(output_path, mode="a", header=first_chunk, index=False)
                first_chunk = False

        if first_chunk:
            print(f"⚠️ No NTP data found in {csv_file}")
        else:
            print(f"✅ Saved: {output_filename}")

    except Exception as e:
        print(f"❌ Error in {csv_file}: {e}")

def process_all():
    files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith(".csv")]
    files.sort()  # serial order

    for idx, file in enumerate(files, start=1):
        filter_ntp_large(file, idx)

if __name__ == "__main__":
    process_all()
    print("\n🎉 All NTP filtered files saved successfully (serial naming)!")
