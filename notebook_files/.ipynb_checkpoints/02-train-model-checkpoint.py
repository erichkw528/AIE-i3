import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
import os
import joblib

print("Loading training data...")
X_train = pd.read_csv('data/X_train.csv')
y_train = pd.read_csv('data/y_train.csv')

# For simplicity, we'll use a subset of features
feature_columns = ['user_id', 'movie_id', 'release_year']
X_train_features = X_train[feature_columns]

# Create a simple pipeline with preprocessing and model
print("Training recommendation model...")
pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler()),
    ('model', Ridge(alpha=1.0))
])

# Train the model
pipeline.fit(X_train_features, y_train.values.ravel())

# Save the model
print("Saving model...")
os.makedirs('models', exist_ok=True)
joblib.dump(pipeline, 'models/movie_recommender.pkl')

print("Model training completed successfully!") 