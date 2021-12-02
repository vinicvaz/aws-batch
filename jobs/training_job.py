import sys
import json
import pandas as pd
import io
import boto3
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from botocore.exceptions import ClientError


s3_client = boto3.client('s3')

def get_s3_dataset(bucket: str, file_path: str) -> pd.DataFrame:

    resp = s3_client.get_object(Bucket=bucket, Key=file_path)
    df = pd.read_csv(resp['Body'], sep=',', index_col=0)
    return df


def upload_data_to_s3(data, dst_path, bucket) -> bool:

    s3_client.put_object(Body=data, Bucket=bucket, Key=dst_path)
    return True


def train_job():
    request = json.loads(sys.argv[1])
    bucket = request.get('s3_bucket')
    file_path = request.get('s3_path')
    target_name = request.get('target_name')

    # Get data from s3 bucket
    df = get_s3_dataset(bucket=bucket, file_path=file_path)

    x = df.drop(target_name, axis=1)
    y = df[target_name]
    X_train, X_val, y_train, y_val = train_test_split(x, y, test_size=.2)

    rf = RandomForestClassifier()
    rf.fit(X_train, y_train)

    p = rf.predict(X_val)
    output_df = X_val.copy()
    output_df['true_values'] = y_val
    output_df['predictions'] = p
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=None)

    # Save validation Results in s3
    upload_data_to_s3(buffer.getvalue(), 'test.csv', bucket)
    print('Job Finished')
    return

if __name__ == '__main__':
    train_job()