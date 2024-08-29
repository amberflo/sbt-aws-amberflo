import boto3
import gzip
import http.client
import json
import os
import time
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler import APIGatewayHttpResolver
from aws_lambda_powertools.event_handler.exceptions import BadRequestError
from aws_lambda_powertools.event_handler.openapi.params import Path
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.shared.types import Annotated
from http import HTTPStatus
from io import BytesIO
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
    meter = make_api_call('POST', '/meters', payload)
    return {'data': meter}, HTTPStatus.CREATED

@app.get("/meters/<meter_id>")
@tracer.capture_method
def fetch_meter(meter_id: Annotated[str, Path(min_length=1)]):
    meter = make_api_call('GET', f'/meters/{meter_id}', None)
    return {'data': meter}, HTTPStatus.OK

@app.get("/meters")
@tracer.capture_method
def fetch_all_meters():
    meters = make_api_call('GET', '/meters', None)
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
    meter = make_api_call('DELETE', f'/meters/{meter_id}', None)
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
        'dimensions': {k: v for k, v in detail.items() if k not in ['meterApiName', 'meterValue']}
    }
    return make_api_call('POST', '/ingest', meter_event)

def decode_response_body(response):
    try:
        if response.getheader('Content-Encoding') == 'gzip':
            # Decompress the response
            response_body = response.read()
            with gzip.GzipFile(fileobj=BytesIO(response_body)) as gz:
                response_body = gz.read().decode('utf-8')
        else:
            # Directly read and decode the response if not compressed
            response_body = response.read().decode('utf-8')

        # Attempt to parse JSON from the response body
        return json.loads(response_body) if response_body else {}
    except json.JSONDecodeError:
        return response_body
    except Exception as e:
        logger.error(f'Error decoding response body: {e}')
        raise

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
            response_body = decode_response_body(response)
            logger.info(f'Response body: {response_body}')

            # Check for HTTP error status
            if response.status >= 400 and response.status < 500:
                raise BadRequestError(response_body)
            elif response.status >= 500:
                raise RuntimeError(f"Server error {response.status}: {response_body}")

            # Process the response and return the result
            return response_body
        except RuntimeError as e:
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
    if 'detail-type' in event and event['detail-type'] == 'ingestUsage':
        return ingest(event)
    return app.resolve(event, context)
