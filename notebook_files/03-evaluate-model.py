import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
import matplotlib.pyplot as plt
import os

print("Loading test data...")
X_test = pd.read_csv('data/X_test.csv')
y_test = pd.read_csv('data/y_test.csv')

print(f"Test data shape: {X_test.shape}")

# Load the trained model and the feature list
print("Loading model...")
pipeline = joblib.load('models/movie_recommender.pkl')
numerical_features = joblib.load('models/feature_list.pkl')

print(f"Using features: {numerical_features}")

# Ensure all required features are in the test data
for feature in numerical_features:
    if feature not in X_test.columns:
        raise ValueError(f"Required feature '{feature}' not found in test data")

# Select features for prediction
X_test_features = X_test[numerical_features]

# Make predictions
print("Making predictions...")
y_pred = pipeline.predict(X_test_features)

# Calculate evaluation metrics
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Model Evaluation Results:")
print(f"Mean Squared Error: {mse:.4f}")
print(f"Root Mean Squared Error: {rmse:.4f}")
print(f"Mean Absolute Error: {mae:.4f}")
print(f"R² Score: {r2:.4f}")

# Create a directory for visualizations
os.makedirs('visualizations', exist_ok=True)

# Create a plot of actual vs predicted ratings
plt.figure(figsize=(10, 6))
plt.scatter(y_test.values, y_pred, alpha=0.3)
plt.plot([min(y_test.values), max(y_test.values)], [min(y_test.values), max(y_test.values)], 'r--')
plt.xlabel('Actual Ratings')
plt.ylabel('Predicted Ratings')
plt.title('Actual vs Predicted Movie Ratings')
plt.savefig('visualizations/actual_vs_predicted.png')

# Histogram of prediction errors
plt.figure(figsize=(10, 6))
errors = y_test.values.ravel() - y_pred
plt.hist(errors, bins=50)
plt.xlabel('Prediction Error')
plt.ylabel('Count')
plt.title('Histogram of Prediction Errors')
plt.savefig('visualizations/error_histogram.png')

# Feature importance (if available)
if hasattr(pipeline[-1], 'feature_importances_'):
    plt.figure(figsize=(12, 8))
    importances = pipeline[-1].feature_importances_
    indices = np.argsort(importances)[::-1]
    plt.bar(range(len(numerical_features)), importances[indices])
    plt.xticks(range(len(numerical_features)), [numerical_features[i] for i in indices], rotation=90)
    plt.title('Feature Importances')
    plt.tight_layout()
    plt.savefig('visualizations/feature_importances.png')

print("Model evaluation completed successfully!") 