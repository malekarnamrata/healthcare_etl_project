import sys
import boto3
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions

# ================================================================
# Glue Job Arguments
# ================================================================
args = getResolvedOptions(
    sys.argv,
    ['SOURCE_BUCKET', 'TARGET_BUCKET', 'ENVIRONMENT', 'SNS_TOPIC_ARN']
)

SOURCE_BUCKET = args['SOURCE_BUCKET']
TARGET_BUCKET = args['TARGET_BUCKET']
ENVIRONMENT = args['ENVIRONMENT']
SNS_TOPIC_ARN = args['SNS_TOPIC_ARN']

RAW_PREFIX = "raw/delta_load"
PROCESSED_PREFIX = "processed/delta_load"

# ================================================================
# AWS Clients
# ================================================================
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

CURRENT_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
results = {}

# ================================================================
# Helper Functions
# ================================================================
def download_csv(filename):
    key = f"{RAW_PREFIX}/{filename}"
    print(f"Reading file: s3://{SOURCE_BUCKET}/{key}")
    local_path = f"/tmp/{filename}"
    s3.download_file(SOURCE_BUCKET, key, local_path)
    return pd.read_csv(local_path)

def upload_csv(df, entity):
    local_path = f"/tmp/{entity}_delta_processed.csv"
    df.to_csv(local_path, index=False)

    s3.upload_file(
        local_path,
        TARGET_BUCKET,
        f"{PROCESSED_PREFIX}/{entity}/{entity}.csv"
    )

def build_dynamodb_item(row):
    item = {}
    for col, val in row.items():
        if pd.isna(val):
            continue
        item[col] = str(val)   # KEEP EVERYTHING AS STRING
    return item

# ================================================================
# DELTA PROCESSING (UPSERT)
# ================================================================
def process_entity_delta(entity, file, table_name, pk, transform_fn):
    try:
        print(f"\nProcessing DELTA for {entity.upper()}")

        df = download_csv(file)

        if df.empty:
            print("No delta data found")
            results[entity] = 0
            return

        df = transform_fn(df)
        upload_csv(df, entity)

        table = dynamodb.Table(table_name)

        with table.batch_writer() as batch:
            for _, row in df.iterrows():
                batch.put_item(Item=build_dynamodb_item(row))

        results[entity] = len(df)
        print(f"✓ {len(df)} records upserted")

    except Exception as e:
        print(f"{entity.upper()} DELTA FAILED:", e)
        results[entity] = "FAILED"

# ================================================================
# TRANSFORMATIONS
# ================================================================
def patients_transform(df):
    df['first_name'] = df['first_name'].str.strip()
    df['last_name'] = df['last_name'].str.strip()
    df['gender'] = df['gender'].fillna('UNKNOWN').str.upper()
    df['email'] = df['email'].str.lower()
    df['insurance_provider'] = df['insurance_provider'].str.upper()
    df['updated_date'] = CURRENT_TIMESTAMP
    df['updated_by'] = 'ETL_USER'
    return df

def doctors_transform(df):
    df['specialization'] = df['specialization'].str.upper()
    df['email'] = df['email'].str.lower()
    df['updated_date'] = CURRENT_TIMESTAMP
    df['updated_by'] = 'ETL_USER'
    return df

def appointments_transform(df):
    df['appointment_date'] = pd.to_datetime(df['appointment_date']).dt.strftime('%Y-%m-%d')
    df['status'] = df['status'].str.upper()
    df['updated_date'] = CURRENT_TIMESTAMP
    df['updated_by'] = 'ETL_USER'
    return df

def treatments_transform(df):
    df['treatment_type'] = df['treatment_type'].str.upper()
    df['updated_date'] = CURRENT_TIMESTAMP
    df['updated_by'] = 'ETL_USER'
    return df

def billing_transform(df):
    df['payment_method'] = df['payment_method'].str.upper()
    df['payment_status'] = df['payment_status'].str.upper()
    df['updated_date'] = CURRENT_TIMESTAMP
    df['updated_by'] = 'ETL_USER'
    return df

# ================================================================
# EXECUTION
# ================================================================
process_entity_delta(
    "patients",
    "delta_patients.csv",
    f"healthcare_patients_{ENVIRONMENT}",
    "patient_id",
    patients_transform
)

process_entity_delta(
    "doctors",
    "delta_doctors.csv",
    f"healthcare_doctors_{ENVIRONMENT}",
    "doctor_id",
    doctors_transform
)

process_entity_delta(
    "appointments",
    "delta_appointments.csv",
    f"healthcare_appointments_{ENVIRONMENT}",
    "appointment_id",
    appointments_transform
)

process_entity_delta(
    "treatments",
    "delta_treatments.csv",
    f"healthcare_treatments_{ENVIRONMENT}",
    "treatment_id",
    treatments_transform
)

process_entity_delta(
    "billing",
    "delta_billing.csv",
    f"healthcare_billing_{ENVIRONMENT}",
    "bill_id",
    billing_transform
)

# ================================================================
# SNS SUMMARY
# ================================================================
sns.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="Delta Load Completed",
    Message=str(results)
)

print("\n✓ DELTA LOAD COMPLETED SUCCESSFULLY")
