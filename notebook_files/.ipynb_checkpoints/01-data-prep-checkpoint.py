import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

print("Creating synthetic movie dataset...")

# Create synthetic movie data
np.random.seed(42)

# Generate user IDs (1-1000)
n_users = 1000
user_ids = np.arange(1, n_users + 1)

# Generate movie IDs (1-200)
n_movies = 200
movie_ids = np.arange(1, n_movies + 1)

# Generate movie titles
movie_genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Thriller', 'Romance', 'Horror']
movie_titles = []
movie_years = []

for i in range(1, n_movies + 1):
    genre = np.random.choice(movie_genres)
    year = np.random.randint(1970, 2020)
    movie_titles.append(f"Movie {i}: {genre} ({year})")
    movie_years.append(year)

# Create movie info dataframe
movies_info = pd.DataFrame({
    'movie_id': movie_ids,
    'movie_title': movie_titles,
    'release_year': movie_years
})

# Generate ratings data (10,000 ratings)
n_ratings = 10000
random_users = np.random.choice(user_ids, n_ratings)
random_movies = np.random.choice(movie_ids, n_ratings)
random_ratings = np.random.randint(1, 6, n_ratings)

# Create ratings dataframe
data = pd.DataFrame({
    'user_id': random_users,
    'movie_id': random_movies,
    'rating': random_ratings
})

# Merge with movie information
data = data.merge(movies_info, on='movie_id')

# Use rating as target
target = data['rating']
data = data.drop('rating', axis=1)

# Create train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    data, target, test_size=0.2, random_state=42
)

# Save the processed data
print("Saving processed data...")
X_train.to_csv('data/X_train.csv', index=False)
X_test.to_csv('data/X_test.csv', index=False)
y_train.to_csv('data/y_train.csv', index=False)
y_test.to_csv('data/y_test.csv', index=False)

print("Data preparation completed successfully!") 