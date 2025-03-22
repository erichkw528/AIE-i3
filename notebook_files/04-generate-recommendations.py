import pandas as pd
import numpy as np
import joblib
import os

print("Loading model and data...")
# Load the trained model and feature list
pipeline = joblib.load('models/movie_recommender.pkl')
numerical_features = joblib.load('models/feature_list.pkl')

# Load the movie data
X_train = pd.read_csv('data/X_train.csv')
X_test = pd.read_csv('data/X_test.csv')
 
# Combine train and test data to get all movies
all_data = pd.concat([X_train, X_test])

print(f"Total unique movies: {all_data['movie_id'].nunique()}")
print(f"Total unique users: {all_data['user_id'].nunique()}")

# Create a movie lookup by title (if movie_title column exists)
if 'movie_title' in all_data.columns:
    movie_lookup = all_data[['movie_id', 'movie_title']].drop_duplicates()
    movie_lookup = movie_lookup.set_index('movie_id')
else:
    # If no movie titles, create a simple mapping
    movie_lookup = pd.DataFrame(
        {'movie_title': [f"Movie {id}" for id in all_data['movie_id'].unique()]}, 
        index=all_data['movie_id'].unique()
    )
    print("Warning: No movie_title column found. Using generic movie titles.")

# Get a list of unique movie IDs and users
unique_movies = all_data['movie_id'].unique()
unique_users = all_data['user_id'].unique()

# Function to generate recommendations for a user
def generate_recommendations(user_id, n_recommendations=5):
    # Create a dataframe with all movies for this user
    user_movies = pd.DataFrame({'user_id': [user_id] * len(unique_movies)})
    user_movies['movie_id'] = unique_movies
    
    # Add required features with placeholder values
    for feature in numerical_features:
        if feature not in user_movies.columns:
            if feature == 'release_year' and 'release_year' in all_data.columns:
                # Use median of release year for each movie if available
                movie_years = all_data.groupby('movie_id')['release_year'].median().to_dict()
                user_movies['release_year'] = user_movies['movie_id'].map(
                    lambda x: movie_years.get(x, all_data['release_year'].median())
                )
            else:
                # For other features, use median value from all_data
                if feature in all_data.columns and feature not in ['user_id', 'movie_id']:
                    user_movies[feature] = all_data[feature].median()
                else:
                    # Default to 0 if we can't get a sensible value
                    user_movies[feature] = 0
    
    # Ensure all required features are present for prediction
    for feature in numerical_features:
        if feature not in user_movies.columns:
            raise ValueError(f"Required feature '{feature}' missing and couldn't be generated")
    
    # Predict ratings
    predicted_ratings = pipeline.predict(user_movies[numerical_features])
    
    # Add predicted ratings to the dataframe
    user_movies['predicted_rating'] = predicted_ratings
    
    # Sort by rating (descending)
    user_movies = user_movies.sort_values('predicted_rating', ascending=False)
    
    # Get the top n recommendations
    top_recommendations = user_movies.head(n_recommendations)
    
    # Add movie titles
    top_recommendations = top_recommendations.join(movie_lookup, on='movie_id')
    
    return top_recommendations[['movie_id', 'movie_title', 'predicted_rating']]

# Create a directory for recommendations
os.makedirs('recommendations', exist_ok=True)

# Generate recommendations for 5 sample users
print("Generating recommendations for sample users...")
sample_users = np.random.choice(unique_users, min(5, len(unique_users)), replace=False)

recommendations_output = pd.DataFrame()
for user_id in sample_users:
    print(f"Generating recommendations for user {user_id}")
    user_recommendations = generate_recommendations(user_id)
    user_recommendations['user_id'] = user_id
    recommendations_output = pd.concat([recommendations_output, user_recommendations])

# Save recommendations to a CSV file
recommendations_output.to_csv('recommendations/sample_recommendations.csv', index=False)

# Generate overall top movies
print("Generating overall top movies...")
all_users_ratings = pd.DataFrame()

# Sample a subset of users if there are too many
sample_size = min(100, len(unique_users))
user_sample = np.random.choice(unique_users, sample_size, replace=False)

for user_id in user_sample:
    # For each user, get predictions for all movies
    user_movies = pd.DataFrame({'user_id': [user_id] * len(unique_movies)})
    user_movies['movie_id'] = unique_movies
    
    # Add required features with placeholder values
    for feature in numerical_features:
        if feature not in user_movies.columns:
            if feature == 'release_year' and 'release_year' in all_data.columns:
                movie_years = all_data.groupby('movie_id')['release_year'].median().to_dict()
                user_movies['release_year'] = user_movies['movie_id'].map(
                    lambda x: movie_years.get(x, all_data['release_year'].median())
                )
            else:
                if feature in all_data.columns and feature not in ['user_id', 'movie_id']:
                    user_movies[feature] = all_data[feature].median()
                else:
                    user_movies[feature] = 0
    
    # Predict ratings
    user_movies['predicted_rating'] = pipeline.predict(user_movies[numerical_features])
    all_users_ratings = pd.concat([all_users_ratings, user_movies])

# Calculate average predicted rating per movie
avg_ratings = all_users_ratings.groupby('movie_id')['predicted_rating'].mean().reset_index()
avg_ratings = avg_ratings.sort_values('predicted_rating', ascending=False)

# Get top 20 movies
top_movies = avg_ratings.head(20)
top_movies = top_movies.join(movie_lookup, on='movie_id')
top_movies.to_csv('recommendations/top_movies_overall.csv', index=False)

print("Recommendation generation completed successfully!") 