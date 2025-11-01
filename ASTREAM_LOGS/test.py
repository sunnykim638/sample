import pandas as pd, matplotlib.pyplot as plt, glob, os

# --- CONFIGURE THESE if needed ---
# Try candidates for per-segment CSV files (pick the one that exists)
candidates = ["DASH_BUFFER_LOG*.csv", "DASH_SEGMENT_LOG*.csv", "DASH_DOWNLINK_LOG*.csv"]
# Expected columns (adjust if your file uses different headers)
# Must include: EpochTime (s), SegmentSizeBytes, DownloadTime (s)
COL_TIME = "EpochTime"
COL_SIZE = "SegmentSizeBytes"
COL_DLT  = "DownloadTime"
# ---------------------------------

files = []
for pat in candidates:
    files.extend(glob.glob(pat))
files = sorted(files)
assert files, "No DASH log CSVs found. Run the client first."

os.makedirs("mobile_figs", exist_ok=True)

for f in files:
    df = pd.read_csv(f)
    # Basic sanity filter
    df = df[[COL_TIME, COL_SIZE, COL_DLT]].dropna()
    df = df[df[COL_DLT] > 0]

    # Throughput per segment (Mbps)
    df["throughput_Mbps"] = (df[COL_SIZE] * 8.0 / df[COL_DLT]) / 1e6

    # Normalize time (start at 0)
    t0 = df[COL_TIME].min()
    df["t"] = df[COL_TIME] - t0

    # Plot
    plt.figure(figsize=(7,4))
    plt.plot(df["t"], df["throughput_Mbps"], 'k:x', linewidth=1)
    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (Mbps)")
    plt.title(f"Throughput over time\n{os.path.basename(f)}")
    plt.grid(True, alpha=0.25)
    out = os.path.join("mobile_figs", os.path.basename(f).replace(".csv","_throughput.png"))
    plt.savefig(out, dpi=180, bbox_inches="tight")
    plt.close()
    print("saved:", out)

print("Done. Figures in ./mobile_figs/")

