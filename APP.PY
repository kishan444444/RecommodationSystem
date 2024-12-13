import pandas as pd
import requests
from RECOMMODATIONSYSTEM.FILES.logger import logging
from RECOMMODATIONSYSTEM.COMPOUNENTS.data_ingestion import Dataingestion
from RECOMMODATIONSYSTEM.COMPOUNENTS.data_transformation import DataTransformation
from RECOMMODATIONSYSTEM.COMPOUNENTS.model_trainer import ModelTrainer


data_ingestion=Dataingestion()

engagement_data=data_ingestion.initiate_data_ingestion()

data_transformation=DataTransformation()

engagement_data_preprocessor=data_transformation.initiated_data_transformation(engagement_data)

engagement_data_preprocessor

trainer = ModelTrainer()


USERNAME_API = "http.username_api"
CATEGORY_ID_API = "http.CATEGORY_ID_API"
MOOD_API = "http.MOOD_API"


def fetch_data(api_url):
    logging.info(f"Fetching data from {api_url}")
    response = requests.get(api_url)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        logging.error(f"Error fetching data from {api_url}, Status Code: {response.status_code}")
        return pd.DataFrame()
    


# Example usage:
username=fetch_data(USERNAME_API)
user_id=fetch_data(CATEGORY_ID_API)
mood=fetch_data(MOOD_API)



recommended_posts, mae, rmse=trainer.initiate_model_training(engagement_data_preprocessor, username, user_id, mood)

# Output the top 10 recommended post IDs
if recommended_posts:
    print(f"Top 10 recommended post IDs for user '{username}' (user ID {user_id}) and mood '{mood}':")
    print(recommended_posts)


