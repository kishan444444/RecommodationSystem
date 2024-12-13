import os
import sys
from RECOMMODATIONSYSTEM.FILES.logger import logging
from RECOMMODATIONSYSTEM.FILES.exception import customexception
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from surprise import SVD, Dataset, Reader, accuracy
import numpy as np
from surprise.model_selection import train_test_split


class ModelTrainer:
    
    def __init__(self):
        pass
        
        
    
    def initiate_model_training(self, engagement_data_preprocessor, username, user_id, mood, alpha=0.6, beta=0.4):
        
        # Step 1: Data Preprocessing
            
            df = pd.read_csv(engagement_data_preprocessor)  # Your engagement dataset

           

            # Ensure 'title' is in string format for TF-IDF processing
            df['title'] = df['title'].astype(str)

            # Step 1: Content-based Filtering (TF-IDF)
            def compute_content_scores(df, mood=None):
                logging.info("Compute content-based scores using TF-IDF.")
                tfidf = TfidfVectorizer(stop_words='english')
                tfidf_matrix = tfidf.fit_transform(df['title'])
                
                if mood:
                    mood_based_post_idx = df[df['mood'].str.lower() == mood.lower()].index
                    if not mood_based_post_idx.empty:
                        content_scores = cosine_similarity(tfidf_matrix[mood_based_post_idx[0]], tfidf_matrix)
                    else:
                        content_scores = cosine_similarity(tfidf_matrix[0], tfidf_matrix)
                else:
                    content_scores = cosine_similarity(tfidf_matrix[0], tfidf_matrix)
                
                return content_scores.ravel()  # Convert to 1D array

            # Step 2: Collaborative Filtering (SVD)
            def train_svd_model(df):
                logging.info("Train an SVD model for collaborative filtering and calculate MAE and RMSE.")
                reader = Reader(rating_scale=(df['engagement_score'].min(), df['engagement_score'].max()))
                data = Dataset.load_from_df(df[['user_id', 'post_id', 'engagement_score']], reader)
                
                trainset, testset = train_test_split(data, test_size=0.2)
                
                model = SVD()
                model.fit(trainset)
                
                predictions = model.test(testset)
                
                # Calculate MAE and RMSE
                mae = accuracy.mae(predictions, verbose=True)
                rmse = accuracy.rmse(predictions, verbose=True)
                
                return model, predictions, mae, rmse

            # Step 3: Hybrid Model
            def hybrid_recommendation(username, user_id, mood, alpha=0.6, beta=0.4):
                """
                Hybrid recommendation system that combines content-based and collaborative filtering.
                Returns:
                    list: List of recommended post IDs for the user.
                """
                logging.info('list: List of recommended post IDs for the user')
                # Step 1: Content-based scores
                content_scores = compute_content_scores(df, mood=mood)
                df['content_score'] = content_scores  # Store the content scores in the DataFrame
                
                # Step 2: Collaborative Filtering
                model, predictions, mae, rmse = train_svd_model(df)  # Get MAE and RMSE here
                
                # Extract collaborative scores for the given user
                user_predictions = [pred for pred in predictions if pred.uid == str(user_id)]
                
                if not user_predictions:
                    print(f"No predictions found for user_id {user_id}, using default predictions.")
                    user_predictions = predictions  # Default to all predictions if user_id not found
                
                # Extract collaborative scores and align them with post IDs
                collaborative_df = pd.DataFrame({
                    'post_id': [pred.iid for pred in user_predictions],
                    'collaborative_score': [pred.est for pred in user_predictions]
                })
                
                # Merge collaborative scores with the main DataFrame
                merged_df = pd.merge(df, collaborative_df, on='post_id', how='left')
                
                # Replace NaN collaborative scores with 0
                merged_df['collaborative_score'].fillna(0, inplace=True)
                
                # Step 3: Calculate the hybrid final score
                merged_df['final_score'] = (alpha * merged_df['content_score']) + (beta * merged_df['collaborative_score'])
                
                # Step 4: Rank and recommend top 10 posts
                recommended_posts = merged_df.sort_values(by='final_score', ascending=False).head(10)
                
                logging.info('Model Training process completed')
                # Return the recommended post IDs
                return recommended_posts['post_id'].tolist(), mae, rmse