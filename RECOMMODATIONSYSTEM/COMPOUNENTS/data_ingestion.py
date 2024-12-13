import numpy as np
import pandas as pd
from pathlib import Path
import os
import sys
from RECOMMODATIONSYSTEM.FILES.logger import logging
from RECOMMODATIONSYSTEM.FILES.exception import customexception
import requests
from dataclasses import dataclass


class Dataingestionconfig:
    engagement_data_path: str = os.path.join("artifacts", "engagement_data.csv")
    
class Dataingestion:
    def __init__(self):
        self.ingestion_config = Dataingestionconfig()
    
    def initiate_data_ingestion(self):
        logging.info("Data ingestion started")
        
        try:
            # Header for API authorization
            headers = {
                'Flic-Token': 'flic_6e2d8d25dc29a4ddd382c2383a903cf4a688d1a117f6eb43b35a1e7fadbb84b8'
            }

            # API Endpoints (Replace with your actual URLs)
            VIEWED_POSTS_API = "https://api.socialverseapp.com/posts/view?page=1&page_size=1000&resonance_algorithm=resonance_algorithm_cjsvervb7dbhss8bdrj89s44jfjdbsjd0xnjkbvuire8zcjwerui3njfbvsujc5if"
            LIKED_POSTS_API = "https://api.socialverseapp.com/posts/like?page=1&page_size=1000&resonance_algorithm=resonance_algorithm_cjsvervb7dbhss8bdrj89s44jfjdbsjd0xnjkbvuire8zcjwerui3njfbvsujc5if"
            INSPIRED_POSTS_API = "https://api.socialverseapp.com/posts/inspire?page=1&page_size=1000&resonance_algorithm=resonance_algorithm_cjsvervb7dbhss8bdrj89s44jfjdbsjd0xnjkbvuire8zcjwerui3njfbvsujc5if"
            RATED_POSTS_API = "https://api.socialverseapp.com/posts/rating?page=1&page_size=1000&resonance_algorithm=resonance_algorithm_cjsvervb7dbhss8bdrj89s44jfjdbsjd0xnjkbvuire8zcjwerui3njfbvsujc5if"
            ALL_POSTS_API = 'https://api.socialverseapp.com/posts/summary/get?page=1&page_size=1000'
            ALL_USER_API = 'https://api.socialverseapp.com/users/get_all?page=1&page_size=1000' 
            
            # 1. Data Retrieval (API Calls)
            def fetch_data(api_url):
                logging.info(f"Fetching data from {api_url}")
                response = requests.get(api_url, headers=headers)
                if response.status_code == 200:
                    return pd.DataFrame(response.json())
                else:
                    logging.error(f"Error fetching data from {api_url}, Status Code: {response.status_code}")
                    return pd.DataFrame()

            # Fetch data from all APIs
            viewed_posts = fetch_data(VIEWED_POSTS_API)
            viewed_posts = pd.json_normalize(viewed_posts.get('posts', []))
            liked_posts = fetch_data(LIKED_POSTS_API)
            liked_posts = pd.json_normalize(liked_posts.get('posts', []))
            inspired_posts = fetch_data(INSPIRED_POSTS_API)
            inspired_posts = pd.json_normalize(inspired_posts.get('posts', []))
            rated_posts = fetch_data(RATED_POSTS_API)
            rated_posts = pd.json_normalize(rated_posts.get('posts', []))
            all_posts = fetch_data(ALL_POSTS_API)
            all_posts = pd.json_normalize(all_posts.get('posts', []))
            all_posts = all_posts[['id', 'slug', 'title', 'identifier']] if not all_posts.empty else pd.DataFrame()
            all_users = fetch_data(ALL_USER_API)
            all_users = pd.json_normalize(all_users.get('users', []))
            all_users = all_users[['id', 'username']] if not all_users.empty else pd.DataFrame()
            
            
            def time_to_seconds(time_str):
                # Extract time part from 'YYYY-MM-DD HH:MM:SS' and split by ':'
                time_part = time_str.split()[1]
                h, m, s = map(int, time_part.split(':'))
                return h * 3600 + m * 60 + s

            viewed_posts['view_time_seconds'] = viewed_posts['viewed_at'].apply(time_to_seconds)
            liked_posts['like_time_seconds'] = liked_posts['liked_at'].apply(time_to_seconds)
            inspired_posts['inspiration_time_seconds'] = inspired_posts['inspired_at'].apply(time_to_seconds)
            rated_posts['rated_posts_time_seconds']=rated_posts['rated_at'].apply(time_to_seconds)



            # Remove duplicates
            viewed_posts.drop_duplicates(subset=['user_id', 'post_id'], inplace=True)
            liked_posts.drop_duplicates(subset=['user_id', 'post_id'], inplace=True)
            inspired_posts.drop_duplicates(subset=['user_id', 'post_id'], inplace=True)
            rated_posts.drop_duplicates(subset=['user_id', 'post_id'], inplace=True)
                        
            
            
            # Merge data to create engagement data
            engagement_data = pd.merge(viewed_posts, liked_posts, on=['user_id', 'post_id', 'id'], how='outer')
            engagement_data = pd.merge(engagement_data, inspired_posts, on=['user_id', 'post_id', 'id'], how='outer')
            engagement_data = pd.merge(engagement_data, rated_posts, on=['user_id', 'post_id', 'id'], how='outer')
            
            video_metadata = pd.merge(all_posts, all_users, on='id', how='outer', suffixes=('_left', '_right'))
            video_metadata = video_metadata.dropna()
            engagement_data = pd.merge(engagement_data, video_metadata, on='id', how='outer')
            
            engagement_data.fillna(0, inplace=True)
            engagement_data.drop(columns=['viewed_at', 'liked_at', 'inspired_at', 'rated_at'], errors='ignore', inplace=True)

            engagement_data['user_id'] = engagement_data['user_id'].astype(int, errors='ignore')
            
            # Save data to CSV
            os.makedirs(os.path.dirname(self.ingestion_config.engagement_data_path), exist_ok=True)
            engagement_data.to_csv(self.ingestion_config.engagement_data_path, index=False)
            
            logging.info("Data ingestion process completed successfully")
            
            
            return self.ingestion_config.engagement_data_path
            
        
        except Exception as e:
            logging.error("Exception occurred during data ingestion", exc_info=True)
            raise customexception(e, sys)