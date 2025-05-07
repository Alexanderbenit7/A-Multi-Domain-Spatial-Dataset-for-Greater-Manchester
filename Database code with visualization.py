import os
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Define paths
csv_folder = 'C:/Users/lenovo/Desktop/Processed datasets-20250417T071252Z-001'
db_path = 'C:/Users/lenovo/Desktop/Processed datasets-20250417T071252Z-001.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Step 2: Identify all CSV files in the folder
csv_files = []
for root, dirs, files in os.walk(csv_folder):
    for file in files:
        if file.endswith('.csv'):
            csv_files.append(os.path.join(root, file))

# Step 3: Import each CSV file into the SQLite database with first column as primary key
imported_tables = []
failed_tables = []

for file_path in csv_files:
    try:
        table_name = os.path.splitext(os.path.basename(file_path))[0].lower().replace(" ", "_")
        df = pd.read_csv(file_path)

        first_col = df.columns[0]
        df[first_col] = df[first_col].astype(str)  # Ensure primary key is string

        cols = []
        for i, col in enumerate(df.columns):
            dtype = 'INTEGER' if pd.api.types.is_integer_dtype(df[col]) else 'REAL' if pd.api.types.is_float_dtype(df[col]) else 'TEXT'
            constraint = 'PRIMARY KEY' if i == 0 else ''
            cols.append(f'"{col}" {dtype} {constraint}'.strip())

        cursor.execute(f"CREATE TABLE {table_name} ({', '.join(cols)})")

        df.to_sql(table_name, conn, if_exists='append', index=False)
        imported_tables.append(table_name)
        print(f"Imported: {table_name}")
    except Exception as e:
        failed_tables.append((file_path, str(e)))
        print(f"Failed to import: {file_path}. Error: {e}")

# Step 4: Commit changes before visualizing
conn.commit()

# Step 5: Visualize CO2 emissions (Top 10)
try:
    df_co2 = pd.read_sql("SELECT * FROM co2_emmissions_2023", conn)
    df_clean = df_co2.dropna(subset=["all_dwellings"])
    df_top10 = df_clean.sort_values(by="all_dwellings", ascending=False).head(10)

    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df_top10,
        x="all_dwellings",
        y="middle_super_output_layer_msoa_name",
        palette="viridis"
    )
    plt.xlabel("All Dwellings Emissions (tonnes CO₂)")
    plt.ylabel("MSOA")
    plt.title("Top 10 MSOAs by All Dwelling CO₂ Emissions (2023)")
    plt.tight_layout()
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.show()
except Exception as e:
    print(f"Visualization failed: {e}")

# Step 6: Close connection
conn.close()

# Step 7: Report import results
print("\nImported tables:")
for table in imported_tables:
    print(f" - {table}")

if failed_tables:
    print("\nFailed imports:")
    for file_path, error in failed_tables:
        print(f" - {file_path}: {error}")
else:
    print("\nAll files were imported successfully.")
