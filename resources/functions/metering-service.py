import os
import http.client
import json
import time
import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import APIGatewayHttpResolver
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from aws_lambda_powertools.event_handler.openapi.params import Path
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.shared.types import Annotated
from botocore.exceptions import ClientError
from http import HTTPStatus
from urllib.parse import urlencode

# Initialize logger and tracer
logger = Logger()
tracer = Tracer()

# Fetch environment variables
API_KEY_SECRET_NAME = os.environ['API_KEY_SECRET_NAME']
API_KEY_SECRET_ID = os.environ['API_KEY_SECRET_ID']
BASE_URL = os.environ['AMBERFLO_BASE_URL']

def fetch_api_key():
    # Fetch the secret from AWS Secrets Manager
    try:
        secrets_client = boto3.client('secretsmanager')
        response = secrets_client.get_secret_value(SecretId=API_KEY_SECRET_NAME)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])[API_KEY_SECRET_ID]
        else:
            raise ClientError("SecretString not defined")
    except KeyError as e:
        logger.error("API key secret not defined")
        raise
    except ClientError as e:
        logger.error(f'Error retrieving secret: {e}')
        raise

API_KEY = fetch_api_key()

app = APIGatewayHttpResolver()

@app.post("/meters")
@tracer.capture_method
def create_meter():
    payload = app.current_event.json_body
    if not all(k in payload for k in ['label', 'meterApiName', 'meterType']):
        raise BadRequestError("Required properties missing for create meter")
    return make_api_call('POST', '/meters', payload)

@app.put("/meters/<meterId>")
@tracer.capture_method
def update_meter():
    payload = app.current_event.json_body
    if not all(k in payload for k in ['label', 'meterApiName', 'meterType']):
        raise BadRequestError("Required properties missing for update meter")
    payload.setdefault('id', meterId)
    return make_api_call('PUT', '/meters', payload)

@app.get("/usage/meterId")
@tracer.capture_method
def fetch_usage():
    payload = app.current_event.query_string_parameters
    if payload['meterApiName'] is None:
        raise BadRequestError("meterApiName is required for fetching usage")

    timeRange = {
        'startTimeInSeconds': payload.get('startTimeInSeconds', int(time.time()) - (24 * 60 * 60)),
        'endTimeInSeconds': payload.get('endTimeInSeconds', None)
    }
    payload.setdefault('timeGroupingInterval', 'DAY')
    payload.setdefault('timeRange', timeRange)

    return make_api_call('POST', '/usage', payload)

@app.delete("/usage")
@tracer.capture_method
def cancel_usage():
    payload = app.current_event.json_body
    if not all(k in payload for k in ['meterApiName', 'id', 'ingestionTimeRange']):
        raise BadRequestError("Required properties missing for cancelUsage request")
    payload['type'] = 'by_property_filter_out'
    return make_api_call('POST', '/ingest-snapshot/custom-filtering-rules', payload)

@tracer.capture_method
def ingest(event):
    detail = event['detail']

    meter = {
        'customerId': detail['tenantId'],
        'meterApiName': detail['meter']['meterApiName'],
        'meterValue': detail['meter']['meterValue'],
        'meterTimeInMillis': int(round(time.time() * 1000)),
        'dimensions': {k: v for k, v in detail['meter'].items() if k not in ['meterApiName', 'meterValue']}
    }
    return make_api_call('POST', '/ingest', meter)

@tracer.capture_method
def make_api_call(method, path, payload, params=None, max_retries = 3):
    conn = http.client.HTTPSConnection(BASE_URL.replace('https://', ''), 443)

    logger.info(f'Making request to {method} {BASE_URL}{path}')

    headers = {
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
        'x-api-key': API_KEY
    }

    # serialize the request body
    payload_str = json.dumps(payload) if payload else None

    # Append query parameters to the path if provided
    if params is not None:
        query_string = urlencode(params)
        path = f"{path}?{query_string}"

    # Retry HTTP requests up to 3 times
    attempt = 1
    while attempt <= max_retries:
        try:
            # Send the request
            conn.request(method, path, payload_str, headers)
            response = conn.getresponse()

            # Log response status
            logger.info(f'Response status: {response.status}')

            # Read and decode the response body
            response_body = response.read().decode('utf-8')
            logger.info(f'Response body: {response_body.replace("\n", "").replace("\r", "")}')

            # Check for HTTP error status
            if response.status >= 400 and response.status < 500:
                raise BadRequestError(response_body)
            elif response.status >= 500:
                raise RuntimeError(f"Server error {response.status}: {response_body}")

            # Process the response and return the result
            return { "statusCode": response.status, 'response': json.loads(json.dumps(response_body)) }
        except RuntimeError as e:
            # Log the exception
            logger.error(f'Error: {e}')

            # Increment attempt count and check retry logic
            attempt += 1
            if attempt <= max_retries:
                logger.info(f'Retrying... Attempt {attempt}/{max_retries}')
            else:
                logger.error(f'Max retries reached. Raising exception.')
                raise e

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_HTTP, log_event=True)
@tracer.capture_lambda_handler
def handler(event, context):
    logger.debug(event)
    if 'detailType' in event and event['detailType'] == 'ingestUsage':
        return ingest(event)
    return app.resolve(event, context)
