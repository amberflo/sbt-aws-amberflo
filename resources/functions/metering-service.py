import boto3
import json
import os
import requests
import time
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import APIGatewayHttpResolver
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from aws_lambda_powertools.event_handler.openapi.params import Path
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.shared.types import Annotated
from http import HTTPStatus
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Initialize logger and tracer
logger = Logger()
tracer = Tracer()

# Fetch environment variables
API_KEY_SECRET_NAME = os.environ['API_KEY_SECRET_NAME']
API_KEY_SECRET_ID = os.environ['API_KEY_SECRET_ID']
BASE_URL = os.environ['AMBERFLO_BASE_URL']

session = requests.Session()
retry = Retry(
    total=3,  # Number of retries
    backoff_factor=1,  # Delay between retries
    status_forcelist=[500, 502, 503, 504]  # Retry on these status codes
)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

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
    meter = make_api_call('POST', '/meters', payload)
    return {'data': meter}, HTTPStatus.CREATED

@app.get("/meters/<meter_id>")
@tracer.capture_method
def fetch_meter(meter_id: Annotated[str, Path(min_length=1)]):
    meter = make_api_call('GET', f'/meters/{meter_id}')
    return {'data': meter}, HTTPStatus.OK

@app.get("/meters")
@tracer.capture_method
def fetch_all_meters():
    meters = make_api_call('GET', '/meters')
    return {'data': meters}, HTTPStatus.OK

@app.put("/meters/<meter_id>")
@tracer.capture_method
def update_meter(meter_id: Annotated[str, Path(min_length=1)]):
    payload = app.current_event.json_body
    if not all(k in payload for k in ['label', 'meterApiName', 'meterType']):
        raise BadRequestError("Required properties missing for update meter")
    payload.setdefault('id', meter_id)
    meter = make_api_call('PUT', '/meters', payload)
    return {'data': meter}, HTTPStatus.OK

@app.delete("/meters/<meter_id>")
@tracer.capture_method
def delete_meter(meter_id: Annotated[str, Path(min_length=1)]):
    meter = make_api_call('DELETE', f'/meters/{meter_id}')
    return {'data': meter}, HTTPStatus.OK

@app.get("/usage/<meter_id>")
@tracer.capture_method
def fetch_usage(meter_id: Annotated[str, Path(min_length=1)]):
    payload = app.current_event.query_string_parameters or {}

    meter_api_name = payload.get('meterApiName')

    # If meterApiName is not provided, fetch it using meterId
    if meter_api_name is None:
        meter_response, _ = fetch_meter(meter_id)
        meter = meter_response.get('data', {})

        # Check if meterApiName was successfully fetched
        meter_api_name = meter.get('meterApiName')
        if meter_api_name is None:
            raise BadRequestError("meterApiName could not be fetched from meterId")
        payload['meterApiName'] = meter_api_name

    timeRange = {
        'startTimeInSeconds': payload.get('startTimeInSeconds', int(time.time()) - (24 * 60 * 60)),
        'endTimeInSeconds': payload.get('endTimeInSeconds', None)
    }
    payload.setdefault('timeGroupingInterval', 'DAY')
    payload.setdefault('timeRange', timeRange)

    usage = make_api_call('POST', '/usage', payload)
    return {'data': usage}, HTTPStatus.OK

@app.delete("/usage")
@tracer.capture_method
def cancel_usage():
    payload = app.current_event.json_body
    if not all(k in payload for k in ['meterApiName', 'id', 'ingestionTimeRange']):
        raise BadRequestError("Required properties missing for cancelUsage request")
    payload['type'] = 'by_property_filter_out'
    filter_rule = make_api_call('POST', '/ingest-snapshot/custom-filtering-rules', payload)
    return {'data': filter_rule}, HTTPStatus.OK

@tracer.capture_method
def ingest(event):
    detail = event['detail']

    meter_event = {
        'customerId': detail['tenantId'],
        'meterApiName': detail['meterApiName'],
        'meterValue': detail['meterValue'],
        'meterTimeInMillis': int(round(time.time() * 1000)),
        'dimensions': {k: v for k, v in detail.items() if k not in ['tenantId', 'meterApiName', 'meterValue']}
    }
    return make_api_call('POST', '/ingest', meter_event)

def decode_response_body(response):
    try:
        response_body = response.text
        # Attempt to parse JSON from the response body
        return response.json() if response_body else {}
    except requests.exceptions.JSONDecodeError as e:
        return response.text
    except Exception as e:
        logger.error(f'Error decoding response body: {e}')
        raise

@tracer.capture_method
def make_api_call(method, path, payload=None, params=None):
    url = f"{BASE_URL}{path}"
    logger.info(f'Making request to {method} {url}')

    headers = {
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
        'x-api-key': API_KEY
    }

    try:
        response = session.request(method, url, json=payload, params=params, headers=headers)

        # Log response status
        logger.info(f'Response status: {response.status_code}')

        # Read and decode the response body
        response_body = decode_response_body(response)
        logger.info(f'Response body: {response_body}')

        if response.status_code >= 400 and response.status_code < 500:
            raise BadRequestError(response_body)

        # Will raise an HTTPError for 4xx/5xx status codes
        response.raise_for_status()
        return response_body
    except requests.HTTPError as http_err:
        logger.error(f'Http error occurred: {http_err}')
        raise RuntimeError(f"Server error {http_err.response.status_code}: {http_err.response.text}")

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_HTTP, log_event=True)
@tracer.capture_lambda_handler
def handler(event, context):
    logger.debug(event)
    if 'detail-type' in event and event['detail-type'] == 'ingestUsage':
        return ingest(event)
    return app.resolve(event, context)
