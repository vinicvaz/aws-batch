from client.batch_client import AWSBatchClient

def app():
    queue = "getting-started-job-queue"
    job_definition = "getting-started-job-definition"
    attempt_duration_seconds = 70
    batch_client = AWSBatchClient(
        job_queue=queue,
        job_definition=job_definition,
        attempt_duration_seconds=attempt_duration_seconds
    )

    # This command override default job entrypoint
    entrypoint_command = ["python", "training_job.py"]
    # Payload to send through cli args
    # Entry point + payload length must be at most 8192
    payload = {
        "s3_bucket": 'aws-batch-tutorial',
        "s3_path": 'iris.csv',
        "target_name": "species_id"
    }
    batch_client_response = batch_client.run(
        job_name='training-iris',
        entrypoint_command=entrypoint_command,
        payload=payload
    )
    return batch_client_response



if __name__ == '__main__':
    app()