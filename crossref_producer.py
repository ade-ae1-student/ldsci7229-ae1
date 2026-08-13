 
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
 
import boto3
 
firehose = boto3.client('firehose')
dynamodb = boto3.resource('dynamodb')
 
BASE_URL = 'https://api.crossref.org/works'
SELECT_FIELDS = 'DOI,title,type,publisher,created,is-referenced-by-count'
 
 
def build_url(rows, from_date, contact_email):
    """Build the CrossRef query validated during API exploration.
 
    sort=created&order=desc  -> newest records first
    filter=from-created-date -> bounds the window so each poll returns
                                recent records, not a random slice of 185M
    mailto                   -> routes the request to the polite pool
    """
    params = {
        'rows': rows,
        'sort': 'created',
        'order': 'desc',
        'filter': f'from-created-date:{from_date}',
        'select': SELECT_FIELDS,
        'mailto': contact_email,
    }
    return f'{BASE_URL}?{urllib.parse.urlencode(params)}'
 
 
def fetch_works(url, contact_email):
    """Fetch and parse the CrossRef response.
 
    urllib is used rather than requests because the Lambda Python runtime
    does not include requests, and adding a layer for one HTTP call is
    unnecessary overhead (Lab 6, Task 12.2).
    """
    req = urllib.request.Request(
        url,
        headers={
            'User-Agent': f'ADE-AE1-Pipeline/1.0 (mailto:{contact_email})'
        }
    )
    with urllib.request.urlopen(req, timeout=25) as response:
        return json.loads(response.read().decode('utf-8'))
 
 
def extract_record(item):
    """Map one raw CrossRef item onto the flat conformed schema.
 
    Every value returned is a string or a number - no nested dicts,
    no lists. Firehose writes whatever it is given, the Glue crawler
    infers the schema from that, and nested values become struct<>
    columns that are awkward to query in Athena.
    """
    # created is a nested object; pull it out once.
    # The {} default means a missing 'created' does not crash the next line.
    created = item.get('created', {})
 
    # title is a LIST, and can be present but empty.
    # 'or' catches both cases because [] and None are both falsy.
    title_list = item.get('title') or ['Untitled']
 
    return {
        'event_id': item.get('DOI', ''),
 
        # Use date-time (always complete ISO 8601), not date-parts,
        # which is variable-length and will IndexError intermittently.
        'event_timestamp': created.get('date-time', ''),
 
        'title': title_list[0],
 
        # category and region become dimension keys, so they must never
        # be null - Lecture 3, rule 10: disallow null keys in facts.
        'category': item.get('type') or 'UNKNOWN',
        'region': item.get('publisher') or 'UNKNOWN',
 
        # double, because this column also carries victim age from the
        # collisions source once the two datasets are merged.
        'metric_value': float(item.get('is-referenced-by-count') or 0),
 
        # Discriminator: identifies the source after the union.
        'source': 'crossref',
    }
 
 
def lambda_handler(event, context):
    start = time.time()
 
    firehose_name = os.environ['FIREHOSE_NAME']
    metadata_table = os.environ['METADATA_TABLE']
    contact_email = os.environ['CONTACT_EMAIL']
    rows = int(os.environ.get('ROWS_PER_RUN', '50'))
 
    from_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    url = build_url(rows, from_date, contact_email)
    print(f'Fetching: {url}')
 
    payload = fetch_works(url, contact_email)
    items = payload['message']['items']
    print(f'Received {len(items)} items from CrossRef')
 
    # Firehose expects one JSON object per line (JSON Lines).
    # The trailing newline is what makes Glue able to read the file.
    records = []
    skipped = 0
    for item in items:
        try:
            records.append({'Data': json.dumps(extract_record(item)) + '\n'})
        except Exception as exc:
            skipped += 1
            print(f'Skipped malformed record: {exc}')
 
    # put_record_batch accepts a maximum of 500 records per call
    
    failed_count = 0
    for i in range(0, len(records), 500):
        chunk = records[i:i + 500]
        response = firehose.put_record_batch(
            DeliveryStreamName=firehose_name,
            Records=chunk
        )
        failed_count += response['FailedPutCount']
        print(f'Sent chunk of {len(chunk)}, failed {response["FailedPutCount"]}')
 
    duration_ms = int((time.time() - start) * 1000)
 
    # Metadata logging for traceability (Lecture 6; AE1 Task 1).
    # Note: DynamoDB rejects Python floats, so duration is cast to int.
    table = dynamodb.Table(metadata_table)
    table.put_item(Item={
        'run_id': context.aws_request_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'pipeline_stage': 'streaming_ingest',
        'source': 'crossref_api',
        'records_sent': len(records),
        'records_skipped': skipped,
        'failed_count': failed_count,
        'duration_ms': duration_ms,
        'firehose_stream': firehose_name,
        'destination': 's3://<bucket>/raw/streaming/',
        'status': 'SUCCESS' if failed_count == 0 else 'PARTIAL_FAILURE',
    })
 
    print(f'Sent {len(records)}, failed {failed_count}, '
          f'skipped {skipped}, {duration_ms} ms')
 
    return {
        'statusCode': 200,
        'records_sent': len(records),
        'failed_count': failed_count,
        'skipped': skipped,
    }
 
