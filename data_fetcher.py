import os
import time
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------ PATHS ------------------
TRAIN_XLSX = "data/train.xlsx"
TEST_XLSX  = "data/test.xlsx"

OUT_TRAIN_DIR = "images/train"
OUT_TEST_DIR  = "images/test"

# ------------------ MAPBOX ------------------
# PASTE YOUR MAPBOX TOKEN BELOW (starts with pk.)
MAPBOX_TOKEN = "pk.eyJ1IjoiYW5pbWVzaC0yOCIsImEiOiJjbWsxamVnZHMwNzZ5M2NzYjV3bm9tNnZpIn0.WBwA8QaYez-4yZYxQmjtcQ"

ZOOM = 18
SIZE = "224x224"
STYLE = "mapbox/satellite-v9"  # satellite imagery

# ------------------ UTILS ------------------
def ensure_dirs():
    os.makedirs(OUT_TRAIN_DIR, exist_ok=True)
    os.makedirs(OUT_TEST_DIR, exist_ok=True)

def build_url(lat, lon):
    # Mapbox expects lon,lat order
    return (
        f"https://api.mapbox.com/styles/v1/{STYLE}/static/"
        f"{lon},{lat},{ZOOM}/{SIZE}"
        f"?access_token={MAPBOX_TOKEN}"
    )

def fetch_one(row, out_dir):
    """Download one image for one row. Returns True if success else False."""
    img_path = os.path.join(out_dir, f"{row['id']}.png")

    # Skip if already downloaded
    if os.path.exists(img_path):
        return True

    url = build_url(row["lat"], row["long"])

    # Retry up to 3 times
    for _ in range(3):
        try:
            r = requests.get(url, timeout=25)
            if r.status_code == 200 and len(r.content) > 2000:
                with open(img_path, "wb") as f:
                    f.write(r.content)
                return True
            else:
                time.sleep(0.4)
        except Exception:
            time.sleep(0.6)

    return False

def fetch_images(xlsx_path, out_dir, max_workers=6, limit=None):
    """
    Download satellite images for properties in an Excel file.
    - max_workers: number of parallel downloads (6 is safe).
    - limit: download only first N rows (fast for deadline).
    """
    df = pd.read_excel(xlsx_path)

    if "lat" not in df.columns or "long" not in df.columns:
        raise ValueError(f"lat/long columns not found in {xlsx_path}")

    # Ensure id column exists
    if "id" not in df.columns:
        df["id"] = df.index.astype(str)

    # Optional limit to avoid hours of downloading
    if limit is not None:
        df = df.head(limit)

    success, fail = 0, 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(fetch_one, row, out_dir) for _, row in df.iterrows()]

        for f in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading → {out_dir}"):
            if f.result():
                success += 1
            else:
                fail += 1

    print(f"\nFinished {out_dir}: success={success}, failed={fail}\n")

# ------------------ MAIN ------------------
if __name__ == "__main__":
    if MAPBOX_TOKEN == "PASTE_YOUR_MAPBOX_TOKEN_HERE":
        raise ValueError("Please paste your Mapbox token into MAPBOX_TOKEN before running!")

    ensure_dirs()

    # ✅ Recommended for fast completion:
    # Start with 3000 train images + 1000 test images
    fetch_images(TRAIN_XLSX, OUT_TRAIN_DIR, max_workers=6, limit=3000)
    fetch_images(TEST_XLSX,  OUT_TEST_DIR,  max_workers=6, limit=1000)

    # If later you want ALL images, comment the above and use:
    # fetch_images(TRAIN_XLSX, OUT_TRAIN_DIR, max_workers=6, limit=None)
    # fetch_images(TEST_XLSX,  OUT_TEST_DIR,  max_workers=6, limit=None)
