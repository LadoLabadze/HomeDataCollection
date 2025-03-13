import pandas as pd

# Define file paths (update these if necessary)
files = {
    "SUPER_VIP": "SUPER_VIP.csv",
    "VIP_PLUS": "VIP_PLUS.csv",
    "VIP": "VIP.csv"
}

# Read each file and add a 'Class' column
dfs = []
for category, file in files.items():
    df = pd.read_csv(file)
    df["Class"] = category  # Add a new column with the category name
    dfs.append(df)

# Merge all DataFrames
merged_df = pd.concat(dfs, ignore_index=True)

# Save the merged file
merged_df.to_csv("merged_properties.csv", index=False)

print("✅ Merging complete! Saved as 'merged_properties.csv'")



