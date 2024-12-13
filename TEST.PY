from RECOMMODATIONSYSTEM.COMPOUNENTS.data_ingestion import Dataingestion
from RECOMMODATIONSYSTEM.COMPOUNENTS.data_transformation import DataTransformation
from RECOMMODATIONSYSTEM.COMPOUNENTS.model_trainer import ModelTrainer

data_ingestion=Dataingestion()

engagement_data=data_ingestion.initiate_data_ingestion()

data_transformation=DataTransformation()

engagement_data_preprocessor=data_transformation.initiated_data_transformation(engagement_data)

engagement_data_preprocessor

trainer = ModelTrainer()

# Example usage:
username = "vishnumurthy"  # User's name
user_id = 114 # Example user ID
mood = "positive"  # Example mood

trainer.initiate_model_training(engagement_data_preprocessor, username, user_id, mood)





