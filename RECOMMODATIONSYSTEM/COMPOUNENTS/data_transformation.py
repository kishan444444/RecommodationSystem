import os
import sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from dataclasses import dataclass
from RECOMMODATIONSYSTEM.FILES.logger import logging
from RECOMMODATIONSYSTEM.FILES.exception import customexception






class Data_transformation_config:
    engagement_data_preprocessor=os.path.join("artifacts","preprocessor.csv")

class DataTransformation:
    def __init__(self):
        self.data_transformation_config=Data_transformation_config()
       
    
        
    def initiated_data_transformation(self,engagement_data_path):
        
        try:
            engagement_data_df=pd.read_csv(engagement_data_path)
            
            
            logging.info("read engagement data completed")
            
            engagement_data=pd.DataFrame(engagement_data_df)
            
            # Calculate a unified engagement score
            scaler = MinMaxScaler()
            engagement_data['engagement_score'] = (
                (engagement_data['view_time_seconds'] * (0.3)) +
                (engagement_data['like_time_seconds'] * (0.3)) +
                (engagement_data['inspiration_time_seconds'].notnull().astype(int) * (0.2)) +
                (engagement_data['rated_posts_time_seconds'] * (0.2))
                
            )
            
            # Normalize engagement scores
            engagement_data['normalized_score'] = scaler.fit_transform(engagement_data[['engagement_score']])

            # Mood Calculation (custom logic)
            engagement_data['mood_score'] = (engagement_data['like_time_seconds'] / engagement_data['view_time_seconds']) * engagement_data['rating_percent']
            engagement_data['mood_score'].fillna(0, inplace=True)
            engagement_data['mood'] = engagement_data['mood_score'].apply(lambda x: 'Positive' if x > 50 else 'Negative')

            logging.info("Applying preprocessing object datasets.")
            engagement_data
            # Save data to CSV
            os.makedirs(os.path.dirname(os.path.join(self.data_transformation_config.engagement_data_preprocessor)),exist_ok=True)
            engagement_data.to_csv(self.data_transformation_config.engagement_data_preprocessor,index=False)
            logging.info("Data preprocessing process completed successfully")
            
            
            return self.data_transformation_config.engagement_data_preprocessor

                        
        except Exception as e:
            logging.info("Exception occured in initiating data transformation")
            raise customexception(e,sys)
        
        
    