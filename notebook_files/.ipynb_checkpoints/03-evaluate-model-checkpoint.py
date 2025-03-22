import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error
import joblib
import matplotlib.pyplot as plt
import os

print("Loading test data...")
X_test = pd.read_csv('data/X_test.csv')
y_test = pd.read_csv('data/y_test.csv')

# Load the trained model
print("Loading model...")
pipeline = joblib.load('models/movie_recommender.pkl')

# Select features for prediction
feature_columns = ['user_id', 'movie_id', 'release_year']
X_test_features = X_test[feature_columns]

# Make predictions
print("Making predictions...")
y_pred = pipeline.predict(X_test_features)

# Calculate evaluation metrics
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
mae = mean_absolute_error(y_test, y_pred)

print(f"Model Evaluation Results:")
print(f"Mean Squared Error: {mse:.4f}")
print(f"Root Mean Squared Error: {rmse:.4f}")
print(f"Mean Absolute Error: {mae:.4f}")

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

print("Model evaluation completed successfully!") 