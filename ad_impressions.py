import pandas as pd
import json

# Simulated Data Ingestion
def load_data(ad_impressions_path, clicks_conversions_path):
    # Load ad impressions JSON data
    with open(ad_impressions_path, 'r') as f:
        ad_impressions = json.load(f)
    
    # Load clicks and conversions CSV data
    clicks_conversions = pd.read_csv(clicks_conversions_path)
    
    return ad_impressions, clicks_conversions

# Data Processing
def process_data(ad_impressions, clicks_conversions):
    # Convert ad impressions to DataFrame
    ad_impressions_df = pd.DataFrame(ad_impressions)
    
    # Convert timestamp columns to datetime
    ad_impressions_df['timestamp'] = pd.to_datetime(ad_impressions_df['timestamp'])
    clicks_conversions['timestamp'] = pd.to_datetime(clicks_conversions['timestamp'])
    
    # Merge (simulate correlation) ad impressions with clicks/conversions on ad_id and user_id
    merged_data = pd.merge(ad_impressions_df, clicks_conversions, on=['ad_id', 'user_id'], how='left', suffixes=('_imp', '_click'))
    
    # Filter to include only records with clicks or conversions
     processed_data = merged_data[merged_data['conversion_type'].notnull()]
    
    return merged_data

# Main function to run the program
def main():
    ad_impressions_path = 'ad_impressions.json'
    clicks_conversions_path = 'clicks_conversions.csv'
    
    try:
        ad_impressions, clicks_conversions = load_data(ad_impressions_path, clicks_conversions_path)
        processed_data = process_data(ad_impressions, clicks_conversions)
        
        # Count conversions by ad_id
        analysis_result = processed_data.groupby('ad_id').agg({'conversion_type': 'count'}).reset_index()
        print(analysis_result)
        return analysis_result
        
    except Exception as e:
        print(f"Error processing data: {e}")

# Run the program
analysis_result=main()
