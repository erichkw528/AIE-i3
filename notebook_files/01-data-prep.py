import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os
import glob

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

print("Processing movie data from Kafka streams...")
print(f"Current working directory: {os.getcwd()}")

# Path to the raw parquet files - modify to use absolute or proper relative path
if os.path.exists('raw_data'):
    raw_data_path = 'raw_data/*.parquet'
elif os.path.exists('../raw_data'):
    raw_data_path = '../raw_data/*.parquet'
else:
    # Try using an absolute path based on notebook_files location
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_path = os.path.join(base_dir, 'raw_data/*.parquet')
    print(f"Looking for parquet files at: {raw_data_path}")

# Read and combine all parquet files
parquet_files = glob.glob(raw_data_path)
print(f"Found {len(parquet_files)} parquet files: {parquet_files}")

# Create empty dataframe to hold all data
all_data = pd.DataFrame()

# Read and combine all parquet files
for file in parquet_files:
    print(f"Reading file: {file}")
    df = pd.read_parquet(file)
    print(f"File {file} contains {len(df)} rows and columns: {df.columns.tolist()}")
    # Print sample data to debug
    print(f"Sample data from this file:")
    print(df.head(2))
    print(f"Data types: {df.dtypes}")
    all_data = pd.concat([all_data, df])

print(f"Total records loaded: {len(all_data)}")
print(f"Final data shape: {all_data.shape}")
print(f"Data columns: {all_data.columns.tolist()}")

# Basic data cleaning
if len(all_data) == 0:
    raise ValueError("No data was loaded from the parquet files. Please check file paths and contents.")

# Remove duplicates if any
all_data = all_data.drop_duplicates()
print(f"Data shape after removing duplicates: {all_data.shape}")

# ===== ADDITIONAL DEBUG - RAW DATA BEFORE CONVERSION =====
print("\n===== RAW DATA BEFORE CONVERSION =====")
print("First 3 rows of raw data:")
print(all_data.head(3))
print("\nRaw data types:")
print(all_data.dtypes)
print("="*50)
# ===== END ADDITIONAL DEBUG =====

# Check data types before conversion
print(f"Data types before conversion: {all_data.dtypes}")
print(f"Sample user_id values: {all_data['user_id'].head(5).tolist()}")
print(f"Sample movie_id values: {all_data['movie_id'].head(5).tolist()}")

# Instead of converting to numeric directly, let's extract user and movie IDs differently
# For user_id and movie_id, we'll convert them to categorical codes
if not pd.api.types.is_numeric_dtype(all_data['user_id']):
    all_data['user_id'] = all_data['user_id'].astype('category').cat.codes
    
if not pd.api.types.is_numeric_dtype(all_data['movie_id']):
    all_data['movie_id'] = all_data['movie_id'].astype('category').cat.codes

# Check for any remaining NaN values
print(f"NaN values count: {all_data.isna().sum()}")

# Extract timestamp features
if 'timestamp' in all_data.columns:
    # First check if timestamp is already a datetime object
    if not pd.api.types.is_datetime64_dtype(all_data['timestamp']):
        all_data['timestamp'] = pd.to_datetime(all_data['timestamp'], errors='coerce')
    
    # Create time-based features
    all_data['day_of_week'] = all_data['timestamp'].dt.dayofweek
    all_data['hour_of_day'] = all_data['timestamp'].dt.hour
    
    # Drop the original timestamp column since it's not useful for ML
    all_data = all_data.drop('timestamp', axis=1)

# Make sure rating is numeric
if not pd.api.types.is_numeric_dtype(all_data['rating']):
    all_data['rating'] = pd.to_numeric(all_data['rating'], errors='coerce')
    # Fill any NaN ratings with the median
    all_data['rating'] = all_data['rating'].fillna(all_data['rating'].median())

# ===== ADDITIONAL DEBUG - PROCESSED DATA AFTER CONVERSION =====
print("\n===== PROCESSED DATA AFTER CONVERSION =====")
print("First 3 rows of processed data:")
print(all_data.head(3))
print("\nProcessed data types:")
print(all_data.dtypes)
print("="*50)
# ===== END ADDITIONAL DEBUG =====

# Extract the target variable
target = all_data['rating'].values
# Drop the rating column from the features dataframe
data = all_data.drop('rating', axis=1)

print(f"Data shape before split: {data.shape}")
print(f"Target shape before split: {target.shape}")

# Create train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    data, target, test_size=0.2, random_state=42
)

print(f"Training set shape: {X_train.shape}")
print(f"Test set shape: {X_test.shape}")

# Save the processed data
print("Saving processed data...")
X_train.to_csv('data/X_train.csv', index=False)
X_test.to_csv('data/X_test.csv', index=False)
pd.DataFrame(y_train, columns=['rating']).to_csv('data/y_train.csv', index=False)
pd.DataFrame(y_test, columns=['rating']).to_csv('data/y_test.csv', index=False)

# ===== ADDITIONAL DEBUG - DATA SAVED TO CSV =====
print("\n===== DATA SAVED TO CSV =====")
print("First 3 rows of X_train.csv:")
print(pd.read_csv('data/X_train.csv').head(3))
print("\nFirst 3 rows of X_test.csv:")
print(pd.read_csv('data/X_test.csv').head(3))
print("\nFirst 3 rows of y_train.csv:")
print(pd.read_csv('data/y_train.csv').head(3))
print("\nFirst 3 rows of y_test.csv:")
print(pd.read_csv('data/y_test.csv').head(3))
print("\nFeatures in saved CSV:")
print(f"X_train columns: {pd.read_csv('data/X_train.csv').columns.tolist()}")
print(f"X_test columns: {pd.read_csv('data/X_test.csv').columns.tolist()}")
print("="*50)
# ===== END ADDITIONAL DEBUG =====

print("Data preparation completed successfully!") 