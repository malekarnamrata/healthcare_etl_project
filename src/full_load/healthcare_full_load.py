import sys
import boto3
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions

# ================================================================
# Glue Job Arguments (KEYWORDS)
# ================================================================
args = getResolvedOptions(
    sys.argv,
    ['SOURCE_BUCKET', 'TARGET_BUCKET', 'ENVIRONMENT', 'SNS_TOPIC_ARN']
)

SOURCE_BUCKET = args['SOURCE_BUCKET']
TARGET_BUCKET = args['TARGET_BUCKET']
ENVIRONMENT = args['ENVIRONMENT']
SNS_TOPIC_ARN = args['SNS_TOPIC_ARN']

RAW_PREFIX = "raw/full_load"
PROCESSED_PREFIX = "processed/full_load"

# ================================================================
# AWS Clients
# ================================================================
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

CURRENT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
results = {}

# ================================================================
# Helper Functions
# ================================================================
def download_csv(filename):
    local_path = f"/tmp/{filename}"
    s3.download_file(SOURCE_BUCKET, f"{RAW_PREFIX}/{filename}", local_path)
    return pd.read_csv(local_path)

def upload_csv(df, entity):
    local_path = f"/tmp/{entity}_processed.csv"
    df.to_csv(local_path, index=False)
    s3.upload_file(
        local_path,
        TARGET_BUCKET,
        f"{PROCESSED_PREFIX}/{entity}/{entity}.csv"
    )

def delete_all_items(table, pk):
    response = table.scan(ProjectionExpression=pk)
    with table.batch_writer() as batch:
        for item in response.get("Items", []):
            batch.delete_item(Key={pk: item[pk]})

# ================================================================
# ENTITY PROCESSING
# ================================================================
def process_entity(entity, file, table_name, pk, transform_fn):
    try:
        print(f"\nProcessing {entity.upper()}")

        df = download_csv(file)
        df = transform_fn(df)

        upload_csv(df, entity)

        table = dynamodb.Table(table_name)

        #  DELETE OLD DATA
        delete_all_items(table, pk)

        #  INSERT NEW DATA
        with table.batch_writer() as batch:
            for _, r in df.iterrows():
                batch.put_item(
                    Item={k: str(r[k]) for k in df.columns}
                )

        results[entity] = len(df)

    except Exception as e:
        print(f"{entity.upper()} FAILED:", e)
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
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth']).dt.strftime('%Y-%m-%d')
    df['registration_date'] = pd.to_datetime(df['registration_date']).dt.strftime('%Y-%m-%d')
    df['created_date'] = CURRENT_TIMESTAMP
    df['created_by'] = 'ETL_USER'
    return df

def doctors_transform(df):
    df['specialization'] = df['specialization'].str.upper()
    df['email'] = df['email'].str.lower()
    df['years_experience'] = df['years_experience'].astype(int)
    df['created_date'] = CURRENT_TIMESTAMP
    df['created_by'] = 'ETL_USER'
    return df

def appointments_transform(df):
    df['appointment_date'] = pd.to_datetime(df['appointment_date']).dt.strftime('%Y-%m-%d')
    df['status'] = df['status'].str.upper()
    df['created_date'] = CURRENT_TIMESTAMP
    df['created_by'] = 'ETL_USER'
    return df

def treatments_transform(df):
    df['treatment_type'] = df['treatment_type'].str.upper()
    df['treatment_date'] = pd.to_datetime(df['treatment_date']).dt.strftime('%Y-%m-%d')
    df['cost'] = df['cost'].astype(float)
    df['created_date'] = CURRENT_TIMESTAMP
    df['created_by'] = 'ETL_USER'
    return df

def billing_transform(df):
    df['bill_date'] = pd.to_datetime(df['bill_date']).dt.strftime('%Y-%m-%d')
    df['payment_method'] = df['payment_method'].str.upper()
    df['payment_status'] = df['payment_status'].str.upper()
    df['amount'] = df['amount'].astype(float)
    df['created_date'] = CURRENT_TIMESTAMP
    df['created_by'] = 'ETL_USER'
    return df

# ================================================================
# EXECUTION
# ================================================================
process_entity(
    "patients",
    "patients.csv",
    f"healthcare_patients_{ENVIRONMENT}",
    "patient_id",
    patients_transform
)

process_entity(
    "doctors",
    "doctors.csv",
    f"healthcare_doctors_{ENVIRONMENT}",
    "doctor_id",
    doctors_transform
)

process_entity(
    "appointments",
    "appointments.csv",
    f"healthcare_appointments_{ENVIRONMENT}",
    "appointment_id",
    appointments_transform
)

process_entity(
    "treatments",
    "treatments.csv",
    f"healthcare_treatments_{ENVIRONMENT}",
    "treatment_id",
    treatments_transform
)

process_entity(
    "billing",
    "billing.csv",
    f"healthcare_billing_{ENVIRONMENT}",
    "bill_id",
    billing_transform
)

# ================================================================
# SNS SUMMARY
# ================================================================
success = sum(1 for v in results.values() if v != "FAILED")
total_records = sum(v for v in results.values() if isinstance(v, int))

message = ["Full Load Completed\n"]
for k, v in results.items():
    message.append(f"{k.upper()}: {v}")

message.append(f"\nTotal Records: {total_records}")
message.append(f"Successful: {success}/5")
message.append(f"Completed: {CURRENT_TIMESTAMP}")

sns.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="✓ Full Load Summary",
    Message="\n".join(message)
)

print("\n✓ FULL LOAD WITH TRANSFORMATIONS COMPLETED")
