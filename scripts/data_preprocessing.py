import pandas as pd

# === Step 1: Load CSVs ===
brent = pd.read_csv('data/raw/brent_prices.csv')
wti = pd.read_csv('data/raw/wti_prices.csv')

# === Step 2: Rename columns ===
brent.rename(columns={'date': 'Date', 'PET.RBRTE.D': 'Brent'}, inplace=True)
wti.rename(columns={'date': 'Date', 'PET.RWTC.D': 'WTI'}, inplace=True)

# === Step 3: Merge on 'Date' ===
merged = pd.merge(brent, wti, on='Date', how='inner')

# === Step 4: Convert Date to datetime ===
merged['Date'] = pd.to_datetime(merged['Date'])

# === Step 5: Sort & clean ===
merged.sort_values('Date', inplace=True)
merged.dropna(inplace=True)

# === Step 6: Calculate Spread ===
#merged['Spread'] = merged['Brent'] - merged['WTI']
merged['Spread'] = 10_000 * (merged['Brent'] / merged['WTI'] - 1)  # Spread in basis points

# === Step 7: Save processed data ===
merged.to_csv('data/processed/spread_data.csv', index=False)

print("Merged dataset saved at data/processed/spread_data.csv")