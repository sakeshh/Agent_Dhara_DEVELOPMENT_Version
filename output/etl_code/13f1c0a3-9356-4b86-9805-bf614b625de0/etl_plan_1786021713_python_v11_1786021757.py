import pandas as pd
import logging
import numpy as np

logger = logging.getLogger("agent_dhara")

def transform_factory_relational_test_json(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Step 1: Trim whitespace for factory_id
    df['factory_id'] = df['factory_id'].astype('string').str.strip()
    
    # Step 2: Trim whitespace for inspection_date
    df['inspection_date'] = df['inspection_date'].astype('string').str.strip()
    
    # Step 3: Trim whitespace for location
    df['location'] = df['location'].astype('string').str.strip()
    
    # Step 4: Trim whitespace for product_id
    df['product_id'] = df['product_id'].astype('string').str.strip()
    
    # Step 5: Trim whitespace for production_qty
    df['production_qty'] = df['production_qty'].astype('string').str.strip()
    
    # Step 6: Cast factory_id to string
    df['factory_id_clean'] = df['factory_id'].astype('string')
    
    # Step 7: Cast location to string
    df['location_clean'] = df['location'].astype('string')
    
    # Step 8: Cast production_qty to double
    df['production_qty_clean'] = pd.to_numeric(df['production_qty'], errors='coerce')
    df['production_qty_validation_status'] = np.select(
        [df['production_qty'].isna(), df['production_qty'].astype(str).str.strip() == ''],
        ['source_null', 'empty'],
        default='valid'
    )
    
    # Step 9: Parse inspection_date
    df['inspection_date_clean'] = pd.to_datetime(df['inspection_date'], format='%Y-%m-%d', errors='coerce')
    df['inspection_date_validation_status'] = np.select(
        [df['inspection_date'].isna(), df['inspection_date_clean'].isna()],
        ['source_null', 'invalid_date'],
        default='valid'
    )
    
    # Step 10: Flag outliers for product_id
    # Placeholder for outlier flagging logic
    df['product_id_outlier_flagged'] = False  # Placeholder for actual outlier logic
    
    # Step 11: Flag duplicate candidates based on product_id, factory_id, and inspection_date
    # Placeholder for duplicate flagging logic
    df['duplicate_candidates_flagged'] = False  # Placeholder for actual duplicate logic
    
    # Step 12: Preserve null for factory_id
    df['factory_id_validation_status'] = np.where(df['factory_id'].isna(), 'source_null', 'valid')
    
    # Step 13: Preserve null for inspection_date
    df['inspection_date_validation_status'] = np.where(df['inspection_date'].isna(), 'source_null', 'valid')
    
    # Step 14: Preserve null for location
    df['location_validation_status'] = np.where(df['location'].isna(), 'source_null', 'valid')
    
    # Step 15: Flag duplicate candidates for location
    # Placeholder for duplicate flagging logic
    df['location_duplicate_candidates_flagged'] = False  # Placeholder for actual duplicate logic
    
    # Step 16: Trim and preserve case for machine_name
    df['machine_name'] = df['machine_name'].astype('string').str.strip()
    
    # Step 17: Nullify punctuation for machine_name
    df['machine_name'] = df['machine_name'].replace(r'^[^\w\s]+$', np.nan, regex=True)
    
    # Step 18: Create normalized match key for machine_name
    df['machine_name_match_key'] = df['machine_name'].str.lower()
    
    # Step 19: Preserve null for machine_name
    df['machine_name_validation_status'] = np.where(df['machine_name'].isna(), 'source_null', 'valid')
    
    # Step 20: Preserve null for product_id
    df['product_id_validation_status'] = np.where(df['product_id'].isna(), 'source_null', 'valid')
    
    # Step 21: Preserve null for production_qty
    df['production_qty_validation_status'] = np.where(df['production_qty'].isna(), 'source_null', 'valid')
    
    logger.info("factory_relational_test_json: row_count=%s", df.shape[0])
    return df

def transform_marketing_relational_test_csv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Step 1: Trim whitespace for campaign_id
    df['campaign_id'] = df['campaign_id'].astype('string').str.strip()
    
    # Step 2: Trim whitespace for channel
    df['channel'] = df['channel'].astype('string').str.strip()
    
    # Step 3: Trim whitespace for factory_id
    df['factory_id'] = df['factory_id'].astype('string').str.strip()
    
    # Step 4: Cast budget to double
    df['budget_clean'] = pd.to_numeric(df['budget'], errors='coerce')
    df['budget_validation_status'] = np.select(
        [df['budget'].isna(), df['budget'].astype(str).str.strip() == ''],
        ['source_null', 'empty'],
        default='valid'
    )
    
    # Step 5: Cast channel to string
    df['channel_clean'] = df['channel'].astype('string')
    
    # Step 6: Parse start_date
    df['start_date_clean'] = pd.to_datetime(df['start_date'], format='%Y-%m-%d', errors='coerce')
    df['start_date_validation_status'] = np.select(
        [df['start_date'].isna(), df['start_date_clean'].isna()],
        ['source_null', 'invalid_date'],
        default='valid'
    )
    
    # Step 7: Flag duplicate candidates based on campaign_id, factory_id, and start_date
    # Placeholder for duplicate flagging logic
    df['duplicate_candidates_flagged'] = False  # Placeholder for actual duplicate logic
    
    # Step 8: Nullify punctuation for budget
    df['budget'] = df['budget'].replace(r'^[^\w\s]+$', np.nan, regex=True)
    
    # Step 9: Preserve null for budget
    df['budget_validation_status'] = np.where(df['budget'].isna(), 'source_null', 'valid')
    
    # Step 10: Preserve null for campaign_id
    df['campaign_id_validation_status'] = np.where(df['campaign_id'].isna(), 'source_null', 'valid')
    
    # Step 11: Preserve null for channel
    df['channel_validation_status'] = np.where(df['channel'].isna(), 'source_null', 'valid')
    
    logger.info("marketing_relational_test_csv: row_count=%s", df.shape[0])
    return df

def transform_sales_relational_test_xml(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Step 1: Trim whitespace for campaign_id
    df['campaign_id'] = df['campaign_id'].astype('string').str.strip()
    
    # Step 2: Trim whitespace for product_id
    df['product_id'] = df['product_id'].astype('string').str.strip()
    
    # Step 3: Trim whitespace for region
    df['region'] = df['region'].astype('string').str.strip()
    
    # Step 4: Trim whitespace for sale_id
    df['sale_id'] = df['sale_id'].astype('string').str.strip()
    
    # Step 5: Cast amount to double
    df['amount_clean'] = pd.to_numeric(df['amount'], errors='coerce')
    df['amount_validation_status'] = np.select(
        [df['amount'].isna(), df['amount'].astype(str).str.strip() == ''],
        ['source_null', 'empty'],
        default='valid'
    )
    
    # Step 6: Cast region to string
    df['region_clean'] = df['region'].astype('string')
    
    # Step 7: Parse sale_date
    df['sale_date_clean'] = pd.to_datetime(df['sale_date'], format='%Y-%m-%d', errors='coerce')
    df['sale_date_validation_status'] = np.select(
        [df['sale_date'].isna(), df['sale_date_clean'].isna()],
        ['source_null', 'invalid_date'],
        default='valid'
    )
    
    # Step 8: Flag duplicate candidates based on sale_id, product_id, and campaign_id
    # Placeholder for duplicate flagging logic
    df['duplicate_candidates_flagged'] = False  # Placeholder for actual duplicate logic
    
    # Step 9: Preserve null for amount
    df['amount_validation_status'] = np.where(df['amount'].isna(), 'source_null', 'valid')
    
    # Step 10: Preserve null for campaign_id
    df['campaign_id_validation_status'] = np.where(df['campaign_id'].isna(), 'source_null', 'valid')
    
    # Step 11: Preserve null for customer
    df['customer_validation_status'] = np.where(df['customer'].isna(), 'source_null', 'valid')
    
    # Step 12: Preserve null for product_id
    df['product_id_validation_status'] = np.where(df['product_id'].isna(), 'source_null', 'valid')
    
    # Step 13: Preserve null for region
    df['region_validation_status'] = np.where(df['region'].isna(), 'source_null', 'valid')
    
    # Step 14: Preserve null for sale_date
    df['sale_date_validation_status'] = np.where(df['sale_date'].isna(), 'source_null', 'valid')
    
    # Step 15: Preserve null for sale_id
    df['sale_id_validation_status'] = np.where(df['sale_id'].isna(), 'source_null', 'valid')
    
    logger.info("sales_relational_test_xml: row_count=%s", df.shape[0])
    return df

if __name__ == "__main__":
    factory_df = pd.read_json(_resolve_data_path("Files/raw/factory_relational_test_json.json"))
    marketing_df = pd.read_csv(_resolve_data_path("Files/raw/marketing_relational_test_csv.csv"))
    sales_df = pd.read_xml(_resolve_data_path("Files/raw/sales_relational_test_xml.xml"), parser='lxml') if 'read_xml' in dir(pd) else pd.read_xml(_resolve_data_path("Files/raw/sales_relational_test_xml.xml"))
    
    factory_cleaned = transform_factory_relational_test_json(factory_df)
    marketing_cleaned = transform_marketing_relational_test_csv(marketing_df)
    sales_cleaned = transform_sales_relational_test_xml(sales_df)
    
    factory_cleaned.to_json("Tables/factory_relational_test_json_clean", orient='records', lines=True, index=False)
    marketing_cleaned.to_csv("Tables/marketing_relational_test_csv_clean", index=False)
    sales_cleaned.to_parquet("Tables/sales_relational_test_xml_clean", index=False)