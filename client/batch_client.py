import boto3
import json


class AWSBatchClient(object):
 
    def __init__(self, job_queue, job_definition, attempt_duration_seconds, **config):

        # Config is not necessarly if running in aws or if your environment have these env vars defined
        # It is an object with:
        # aws_access_key_id = AWS_ACCESS_KEY
        # aws_secret_access_key = AWS_SECRET_ACCESS_KEY
        # region_name = AWS_DEFAULT_REGION
    
        self.client = boto3.client("batch", **config)
        self.set_queue(job_queue) # Queue name or arn to send job
        self.set_job_definition(job_definition) # Job definition name or ARN to use
        self.set_attempt_duration(attempt_duration_seconds)

    def set_queue(self, queue):
        self.job_queue = queue
    
    def set_job_definition(self, job_definition):
        self.job_definition = job_definition

    def set_attempt_duration(self, attempt_duration_seconds):
        self.attempt_duration_secods = max(int(attempt_duration_seconds), 60)

    def run(self, job_name, entrypoint_command=None, environment_override=None, payload=None):
        if entrypoint_command is None:
            entrypoint_command = list()
        
        if environment_override is None:
            environment_override = []
        if payload is not None:
            payload = json.dumps(payload)
            entrypoint_command.append(payload)
        
        kwargs = dict(
            jobName=job_name,
            jobQueue=self.job_queue,
            jobDefinition=self.job_definition,
            timeout={'attemptDurationSeconds': self.attempt_duration_secods},
        )

        if entrypoint_command:
            kwargs['containerOverrides'] = {
                "command": entrypoint_command
            }
        if environment_override:
            kwargs['containerOverrides']['environment'] = [e for e in environment_override]

        response = self.client.submit_job(**kwargs)
        return response
