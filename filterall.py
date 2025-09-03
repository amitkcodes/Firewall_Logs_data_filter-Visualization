import os
import re
import zstandard as zstd
import pandas as pd

INPUT_FOLDER = r"E:\data\data1"   # .zst files location
OUTPUT_FOLDER = r"E:\test1\outputall"  # output CSVs location
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def decompress_zst(input_path):
    """Decompress .zst file into text lines"""
    dctx = zstd.ZstdDecompressor()
    with open(input_path, 'rb') as ifh:
        with dctx.stream_reader(ifh) as reader:
            data = reader.read().decode("utf-8", errors="ignore")
            return data.splitlines()

def parse_log_line(line):
    """Extract key=value pairs from one log line"""
    # key="value with spaces" OR key=value
    matches = re.findall(r'(\w+)=("[^"]+"|\S+)', line)
    return {k: v.strip('"') for k, v in matches}

def process_file(zst_file):
    """Process one .zst file into a CSV"""
    input_path = os.path.join(INPUT_FOLDER, zst_file)
    output_path = os.path.join(OUTPUT_FOLDER, zst_file.replace(".zst", ".csv"))

    print(f"[PROCESSING] {zst_file} ...")
    lines = decompress_zst(input_path)

    records = []
    for line in lines:
        record = parse_log_line(line)
        if record:
            records.append(record)

    if records:
        df = pd.DataFrame(records)
        df.to_csv(output_path, index=False)
        print(f"✅ Saved: {output_path}")
    else:
        print(f"⚠️ No valid logs found in {zst_file}")

def process_all():
    for file in os.listdir(INPUT_FOLDER):
        if file.endswith(".zst"):
            process_file(file)

if __name__ == "__main__":
    process_all()
    print("\n🎉 All files processed successfully!")
