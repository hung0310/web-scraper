import pandas as pd
from datetime import datetime

def normalize_time(text):
    if pd.isna(text):
        return None

    text = str(text).strip()

    try:
        dt = datetime.strptime(text.replace(" GMT+7", ""), "%d/%m/%Y %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        pass

    try:
        parts = text.split(",")
        if len(parts) >= 3:
            date_part = parts[1].strip()
            time_part = parts[2].split("(")[0].strip()
            merged = f"{date_part} {time_part}"
            dt = datetime.strptime(merged, "%d/%m/%Y %H:%M")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        pass

    return None


files = {
    "tuoitre": "dataset_paper_tuoitre.csv",
    # "vnexpress": "dataset_paper_vnexpress.csv",
    "znews": "dataset_paper_znews.csv"
}

all_rows = []

for source, file in files.items():
    df = pd.read_csv(file, engine="python", on_bad_lines="skip")
    df["Source"] = source
    df["Time"] = df["Time"].apply(normalize_time)
    all_rows.append(df)

df_all = pd.concat(all_rows, ignore_index=True)

df_all.to_csv("summary_paper.csv", index=False)

print("Đã tạo file summary_paper.csv thành công!")