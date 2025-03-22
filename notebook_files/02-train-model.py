import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import os
import joblib

print("Loading training data...")
X_train = pd.read_csv('data/X_train.csv')
y_train = pd.read_csv('data/y_train.csv')

print(f"Training data shape: {X_train.shape}")
print(f"Training data columns: {X_train.columns.tolist()}")

# Select numerical features for the model
# User ID and movie ID are essential for collaborative filtering
numerical_features = ['user_id', 'movie_id']

# Add release_year if available
if 'release_year' in X_train.columns:
    numerical_features.append('release_year')

# Add any other numerical features that might be in the Kafka data
for col in X_train.columns:
    if X_train[col].dtype in ['int64', 'float64'] and col not in numerical_features:
        numerical_features.append(col)

print(f"Using features: {numerical_features}")
X_train_features = X_train[numerical_features]

# Create a pipeline with preprocessing and model
print("Training recommendation model...")
pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler()),
    ('model', RandomForestRegressor(n_estimators=100, random_state=42))
])

# Train the model
pipeline.fit(X_train_features, y_train.values.ravel())

# Save the model and the feature list
print("Saving model...")
os.makedirs('models', exist_ok=True)
joblib.dump(pipeline, 'models/movie_recommender.pkl')
joblib.dump(numerical_features, 'models/feature_list.pkl')

print("Model training completed successfully!") 