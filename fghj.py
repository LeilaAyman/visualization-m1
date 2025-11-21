import pandas as pd

PATH = r"C:\Users\maria\Downloads\visulaization\final_cleaned.csv"

print(">>> LOADING DATA...")
df = pd.read_csv(PATH)

print("\n>>> SHAPE:", df.shape)

print("\n>>> RAW VALUES FROM vehicle_type_code_1 (with NaNs):")
print(df["vehicle_type_code_1"].value_counts(dropna=False))

print("\n>>> UNIQUE VALUES (exactly as stored):")
vals = df["vehicle_type_code_1"].dropna().unique()
for v in sorted(vals):
    print(repr(v))

print("\n>>> NORMALIZED (upper + stripped) VALUES:")
cleaned = df["vehicle_type_code_1"].astype(str).str.upper().str.strip().unique()
for v in sorted(cleaned):
    print(repr(v))

print("\n>>> DONE.")
