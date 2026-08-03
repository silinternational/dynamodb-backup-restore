import json
import boto3
import gzip
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from decimal import Decimal
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Explicit timeouts so a stalled network call can't hang the Lambda invocation
# until it times out on its own.
BOTO_CONFIG = Config(connect_timeout=10, read_timeout=30)

JSON_GZ_EXTENSION = '.json.gz'
JSON_EXTENSION = '.json'
DATA_FILE_EXTENSIONS = (JSON_GZ_EXTENSION, JSON_EXTENSION)

# Restoring is done synchronously in-process (unlike the DynamoDB-managed
# async export used for backups), so a table/table-set that doesn't finish
# within one invocation has to be checkpointed and continued in a fresh
# invocation rather than left to be killed mid-write by the Lambda timeout.
# RESTORE_TIME_SAFETY_MS is how much runway we reserve to stop cleanly and
# self-invoke before that happens. MAX_RESTORE_CONTINUATIONS bounds how many
# times we'll keep re-invoking before giving up and marking the remainder FAILED.
RESTORE_TIME_SAFETY_MS = 90_000
MAX_RESTORE_CONTINUATIONS = 10

# Initialize AWS clients outside the handler so they're reused across
# invocations on a warm Lambda container.
dynamodb = boto3.client('dynamodb', config=BOTO_CONFIG)
_default_s3_client = boto3.client('s3', config=BOTO_CONFIG)
lambda_client = boto3.client('lambda', config=BOTO_CONFIG)

# The B2 client depends on env vars only present in B2 mode, so it can't be
# built unconditionally at import time - cache it lazily instead so it's only
# built once per warm container.
_b2_client_cache = None


def get_storage_client(mode='s3'):
    """Get storage client based on mode"""
    global _b2_client_cache

    if mode.lower() != 'b2':
        return _default_s3_client

    if _b2_client_cache is None:
        try:
            _b2_client_cache = boto3.client(
                's3',
                endpoint_url='https://s3.us-west-004.backblazeb2.com',
                aws_access_key_id=os.environ['B2_APPLICATION_KEY_ID'],
                aws_secret_access_key=os.environ['B2_APPLICATION_KEY'],
                config=BOTO_CONFIG
            )
        except KeyError as e:
            logger.exception("B2 credentials not found")
            raise ValueError(
                "B2 mode requires B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY environment variables"
            ) from e

    return _b2_client_cache


def decimal_default(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _time_running_low(context, safety_ms=RESTORE_TIME_SAFETY_MS):
    """True once the invocation has less than safety_ms of execution time left"""
    if context is None:
        return False
    try:
        return context.get_remaining_time_in_millis() < safety_ms
    except Exception:
        return False


def validate_environment():
    """Validate required environment variables"""
    required_vars = ['BACKUP_BUCKET', 'ENVIRONMENT']
    missing_vars = []

    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    s3_prefix = os.environ.get('S3_EXPORTS_PREFIX', 'native-exports')
    
    return {
        'backup_bucket': os.environ['BACKUP_BUCKET'],
        'environment': os.environ['ENVIRONMENT'],
        's3_prefix': s3_prefix
    }


def get_tables_to_restore():
    """
    Get list of tables that can be restored from environment variable
    """
    try:
        tables_json = os.environ.get('DYNAMODB_TABLES')
        if not tables_json:
            raise ValueError("DYNAMODB_TABLES environment variable not found")
        
        tables = json.loads(tables_json)
        logger.info(f"Available tables for restore: {tables}")
        return tables

    except json.JSONDecodeError as e:
        logger.exception("Failed to parse DYNAMODB_TABLES")
        raise ValueError(f"Invalid DYNAMODB_TABLES format: {e}") from e
    except Exception:
        logger.exception("Failed to get tables to restore")
        raise


def get_available_backups(s3_client, s3_bucket, s3_prefix='native-exports'):
    """
    Get list of available backup dates
    Returns dates in descending order (newest first)
    """
    try:
        logger.info(f"Scanning for backups in {s3_bucket}/{s3_prefix}/")

        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=f'{s3_prefix}/',
            Delimiter='/'
        )

        backup_dates = []
        for prefix in response.get('CommonPrefixes', []):
            date_part = prefix['Prefix'].rstrip('/').split('/')[-1]
            if len(date_part) == 10 and date_part.count('-') == 2:  # YYYY-MM-DD
                backup_dates.append(date_part)

        sorted_dates = sorted(backup_dates, reverse=True)
        logger.info(f"Found {len(sorted_dates)} backup dates")
        return sorted_dates

    except Exception:
        logger.exception("Failed to get available backups")
        raise


def get_backup_manifest(s3_client, s3_bucket, backup_date, s3_prefix='native-exports'):
    """
    Get backup manifest for a specific date
    """
    try:
        manifest_key = f"{s3_prefix}/{backup_date}/manifest.json"
        logger.info(f"Fetching manifest: {manifest_key}")

        try:
            response = s3_client.get_object(Bucket=s3_bucket, Key=manifest_key)
            manifest = json.loads(response['Body'].read().decode('utf-8'))

            if not isinstance(manifest, dict) or 'exports' not in manifest:
                raise ValueError("Invalid manifest structure")

            logger.info(f"Found valid manifest with {len(manifest.get('exports', []))} exports")
            return manifest

        except s3_client.exceptions.NoSuchKey:
            logger.error(f"Manifest not found: {manifest_key}")
            return None

    except Exception:
        logger.exception("Failed to get backup manifest")
        return None


def validate_export_info(export_info):
    """
    Validate that export_info has required fields and correct status
    """
    if not isinstance(export_info, dict):
        raise ValueError("Export info is not a dictionary")

    required_fields = ['table_name', 's3_prefix', 'status']

    for field in required_fields:
        if field not in export_info:
            raise ValueError(f"Export info missing required field: {field}")

        if not export_info[field]:
            raise ValueError(f"Export info field '{field}' is empty")

    if export_info['status'] != 'COMPLETED':
        raise ValueError(f"Export status is '{export_info['status']}', expected 'COMPLETED'")

    return True


def _list_data_files(s3_client, s3_bucket, prefix):
    """List every object under a prefix and keep only data files"""
    paginator = s3_client.get_paginator('list_objects_v2')
    return [
        obj['Key']
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix)
        for obj in page.get('Contents', [])
        if obj['Key'].endswith(DATA_FILE_EXTENSIONS)
    ]


def _list_common_prefixes(s3_client, s3_bucket, prefix, delimiter='/'):
    """List every CommonPrefixes entry under a prefix (paginated)"""
    paginator = s3_client.get_paginator('list_objects_v2')
    return [
        common_prefix['Prefix']
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix, Delimiter=delimiter)
        for common_prefix in page.get('CommonPrefixes', [])
    ]


def _find_data_files_standard_structure(s3_client, s3_bucket, s3_prefix):
    """Strategy 1: standard s3_prefix/AWSDynamoDB/{export-id}/data/*.json(.gz) layout"""
    data_prefix = f"{s3_prefix}/AWSDynamoDB/"
    logger.info(f"Trying standard structure: {data_prefix}")

    export_dirs = sorted(_list_common_prefixes(s3_client, s3_bucket, data_prefix), reverse=True)
    if not export_dirs:
        return None

    # Use the most recent export directory (highest timestamp)
    export_dir = export_dirs[0]
    logger.info(f"Found export directory: {export_dir}")

    possible_data_paths = [
        f"{export_dir}data/",  # Standard location
        export_dir,  # Files directly in export dir
    ]

    for data_path in possible_data_paths:
        logger.info(f"Checking for data files in: {data_path}")
        data_files = _list_data_files(s3_client, s3_bucket, data_path)
        if data_files:
            logger.info(f" Found {len(data_files)} data files in {data_path}")
            return data_files

    return None


def _find_data_files_direct(s3_client, s3_bucket, s3_prefix):
    """Strategy 2: data files directly under s3_prefix"""
    logger.info(f"Trying direct files under: {s3_prefix}")
    data_files = _list_data_files(s3_client, s3_bucket, f"{s3_prefix}/")
    if data_files:
        logger.info(f" Found {len(data_files)} data files directly under {s3_prefix}")
    return data_files or None


def _find_data_files_recursive(s3_client, s3_bucket, s3_prefix):
    """Strategy 3: recursive search under s3_prefix"""
    logger.info(f"Trying recursive search under: {s3_prefix}")

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=f"{s3_prefix}/")

    data_files = [
        obj['Key']
        for page in pages
        for obj in page.get('Contents', [])
        if obj['Key'].endswith(DATA_FILE_EXTENSIONS)
    ]

    if data_files:
        logger.info(f" Found {len(data_files)} data files via recursive search")
        return data_files
    return None


def _log_missing_data_files_debug(s3_client, s3_bucket, s3_prefix):
    """Best-effort listing of what's actually under s3_prefix, to help diagnose a miss"""
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=f"{s3_prefix}/", MaxKeys=20)
        logger.info(f"Debug: Contents under {s3_prefix}/:")
        for obj in response.get('Contents', []):
            logger.info(f"   {obj['Key']}")
    except Exception:
        logger.debug("Failed to list debug contents for %s", s3_prefix, exc_info=True)


def get_export_data_files(s3_client, s3_bucket, export_info):
    """
    Get list of all data files for an export with robust S3 structure detection
    """
    try:
        validate_export_info(export_info)

        s3_prefix = export_info['s3_prefix'].rstrip('/')
        table_name = export_info['table_name']

        logger.info(f"Looking for data files for {table_name} under: {s3_prefix}")

        search_strategies = (
            ("standard structure", _find_data_files_standard_structure),
            ("direct file search", _find_data_files_direct),
            ("recursive search", _find_data_files_recursive),
        )

        for strategy_name, strategy in search_strategies:
            try:
                data_files = strategy(s3_client, s3_bucket, s3_prefix)
                if data_files:
                    return data_files
            except Exception as e:
                logger.warning(f"{strategy_name} failed: {str(e)}")

        logger.error(f" No data files found for {table_name} under any search strategy")
        _log_missing_data_files_debug(s3_client, s3_bucket, s3_prefix)

        raise RuntimeError(f"No data files found for export {table_name}")

    except Exception:
        logger.exception(
            f"Failed to get export data files for {export_info.get('table_name', 'unknown')}"
        )
        raise


def _read_export_file_content(s3_key, response):
    """Read an export file's body, transparently decompressing .gz files"""
    if s3_key.endswith('.gz'):
        return gzip.decompress(response['Body'].read()).decode('utf-8')
    return response['Body'].read().decode('utf-8')


def _parse_dynamodb_export_lines(content, s3_key):
    """Parse each newline-delimited JSON line of an export file into an item"""
    items = []
    line_count = 0
    error_count = 0

    for line in content.strip().split('\n'):
        line_count += 1
        if not line.strip():
            continue
        try:
            item_data = json.loads(line)
            if 'Item' in item_data:
                items.append(item_data['Item'])
            elif isinstance(item_data, dict):
                # Handle case where the line is already the item
                items.append(item_data)
        except json.JSONDecodeError as e:
            error_count += 1
            if error_count <= 5:  # Log first 5 errors only
                logger.warning(f"JSON decode error on line {line_count}: {str(e)}")

    if error_count > 0:
        logger.warning(f"File {s3_key}: {error_count} JSON decode errors out of {line_count} lines")

    return items


def parse_dynamodb_json_file(s3_client, s3_bucket, s3_key):
    """
    Parse a single DynamoDB JSON export file from S3.
    Returns (items, file_failed). file_failed=True means the file could not be
    downloaded/decompressed/read at all, as distinct from an empty file.
    """
    try:
        logger.debug(f"Parsing file: {s3_key}")

        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        content = _read_export_file_content(s3_key, response)
        items = _parse_dynamodb_export_lines(content, s3_key)

        logger.debug(f"Parsed {len(items)} items from {s3_key}")
        return items, False

    except Exception:
        logger.exception(f"Error parsing file {s3_key}")
        return [], True


def _get_table_key_schema(table_name):
    """Return (partition_key, sort_key) attribute names for a table"""
    response = dynamodb.describe_table(TableName=table_name)
    key_schema = response['Table']['KeySchema']

    partition_key = None
    sort_key = None
    for key in key_schema:
        if key['KeyType'] == 'HASH':
            partition_key = key['AttributeName']
        elif key['KeyType'] == 'RANGE':
            sort_key = key['AttributeName']

    if not partition_key:
        raise RuntimeError("Could not determine partition key")

    return partition_key, sort_key


def _delete_request_for_item(item, partition_key, sort_key):
    key = {partition_key: item[partition_key]}
    if sort_key and sort_key in item:
        key[sort_key] = item[sort_key]
    return {'DeleteRequest': {'Key': key}}


def _delete_batch_with_retries(table_name, delete_requests, max_retries=3):
    """Delete one batch of up to 25 keys, retrying unprocessed requests and exceptions with backoff"""
    pending = delete_requests
    batch_size = len(pending)

    for attempt in range(max_retries):
        try:
            response = dynamodb.batch_write_item(RequestItems={table_name: pending})
        except Exception:
            if attempt >= max_retries - 1:
                logger.exception(f"Batch delete failed after {max_retries} attempts")
                return batch_size - len(pending)
            logger.warning(f"Batch delete attempt {attempt + 1} failed, retrying...", exc_info=True)
            time.sleep(min(2 ** attempt, 10))
            continue

        pending = response.get('UnprocessedItems', {}).get(table_name, [])
        if not pending:
            return batch_size
        if attempt < max_retries - 1:
            time.sleep(min(2 ** attempt, 10))

    logger.warning(f"{len(pending)} items could not be deleted after {max_retries} attempts")
    return batch_size - len(pending)


def _delete_item_page(table_name, items, partition_key, sort_key):
    """Delete one scanned page of items in batches of 25, in parallel"""
    items_deleted = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        delete_futures = []

        for i in range(0, len(items), 25):  # DynamoDB batch limit
            batch = items[i:i + 25]
            delete_requests = [_delete_request_for_item(item, partition_key, sort_key) for item in batch]
            future = executor.submit(_delete_batch_with_retries, table_name, delete_requests)
            delete_futures.append(future)

        for future in as_completed(delete_futures):
            try:
                items_deleted += future.result()
            except Exception:
                logger.exception("Error in delete batch")

    return items_deleted


def clear_existing_table_data(table_name, context=None, start_key=None):
    """
    Clear existing table data before restore
    WARNING: This deletes all existing data!

    Returns (success, items_deleted, last_evaluated_key, interrupted). When
    interrupted is True, last_evaluated_key can be passed back in as start_key
    to resume the scan where it left off - deletes are idempotent, so resuming
    (or even restarting from scratch) never risks double-deleting anything.
    """
    items_deleted = 0
    try:
        logger.warning(f" CLEARING ALL DATA from table: {table_name}")

        partition_key, sort_key = _get_table_key_schema(table_name)
        logger.info(f"Table schema - Partition key: {partition_key}, Sort key: {sort_key}")

        batch_count = 0

        paginate_kwargs = {'TableName': table_name}
        if start_key:
            paginate_kwargs['ExclusiveStartKey'] = start_key

        paginator = dynamodb.get_paginator('scan')
        for page in paginator.paginate(**paginate_kwargs):
            items = page.get('Items', [])
            if items:
                items_deleted += _delete_item_page(table_name, items, partition_key, sort_key)
                batch_count += 1
                if batch_count % 10 == 0:
                    logger.info(f"Deletion progress: ~{items_deleted} items deleted...")

            last_evaluated_key = page.get('LastEvaluatedKey')
            if last_evaluated_key and _time_running_low(context):
                logger.warning(
                    f"Pausing delete for {table_name} with {items_deleted} items deleted so far "
                    f"to avoid hitting the Lambda timeout mid-scan"
                )
                return True, items_deleted, last_evaluated_key, True

        logger.info(f" Successfully cleared {items_deleted} items from {table_name}")
        return True, items_deleted, None, False

    except Exception:
        logger.exception(" Failed to clear table data")
        return False, items_deleted, None, False


def _attempt_batch_write(table_name, put_requests, batch_size, attempt, max_retries):
    """
    Try one batch_write_item call. Returns (successful, failed, retry_requests).
    retry_requests is non-None only when the caller should retry with those requests.
    """
    response = dynamodb.batch_write_item(RequestItems={table_name: put_requests})
    unprocessed = response.get('UnprocessedItems', {}).get(table_name, [])

    if not unprocessed:
        return batch_size, 0, None

    if attempt < max_retries - 1:
        logger.debug(f"Batch had {len(unprocessed)} unprocessed items, retrying... (attempt {attempt + 1})")
        return batch_size - len(unprocessed), 0, unprocessed

    logger.warning(f"Final attempt: {len(unprocessed)} items failed after {max_retries} retries")
    return batch_size - len(unprocessed), len(unprocessed), None


def _write_batch_with_retries(table_name, batch_items, max_retries=3):
    """Write a single batch of items, retrying unprocessed items with backoff"""
    batch_size = len(batch_items)
    put_requests = [{'PutRequest': {'Item': item}} for item in batch_items]

    try:
        for attempt in range(max_retries):
            try:
                successful, failed, retry_requests = _attempt_batch_write(
                    table_name, put_requests, batch_size, attempt, max_retries
                )
                if retry_requests is None:
                    return successful, failed
                time.sleep(min(2 ** attempt, 10))  # Exponential backoff
                put_requests = retry_requests
            except Exception:
                if attempt >= max_retries - 1:
                    raise
                logger.warning(f"Batch write attempt {attempt + 1} failed, retrying...", exc_info=True)
                time.sleep(min(2 ** attempt, 10))
    except Exception:
        logger.exception("Batch write failed after all retries")
        return 0, batch_size

    return 0, batch_size


def batch_write_items_to_table(table_name, items, max_workers=5):
    """Write items to DynamoDB table using batch_write_item with threading"""
    if not items:
        return 0, 0

    total_items = len(items)
    items_written = 0
    failed_items = 0

    logger.info(f"Writing {total_items} items to {table_name} using {max_workers} threads")

    # Process items in batches of 25 (DynamoDB limit) using threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_write_batch_with_retries, table_name, items[i:i + 25])
            for i in range(0, total_items, 25)
        ]

        batch_count = 0
        for future in as_completed(futures):
            try:
                successful, failed = future.result()
                items_written += successful
                failed_items += failed
                batch_count += 1

                if batch_count % 20 == 0:  # Progress every 500 items (20 batches)
                    logger.info(f"Progress: {items_written + failed_items}/{total_items} items processed")

            except Exception:
                logger.exception("Thread execution failed")
                failed_items += 25  # Assume whole batch failed

    success_rate = (items_written / total_items * 100) if total_items > 0 else 0
    logger.info(f"Batch write completed: {items_written}/{total_items} successful ({success_rate:.1f}%)")

    if failed_items > 0:
        logger.warning(f" {failed_items} items failed to write")

    return items_written, failed_items


def count_table_items(table_name):
    """
    Get an exact item count for a table via a Select=COUNT scan.
    Expensive on large tables (reads every item) - only call when explicitly requested.
    """
    total = 0
    paginator = dynamodb.get_paginator('scan')

    for page in paginator.paginate(TableName=table_name, Select='COUNT'):
        total += page.get('Count', 0)

    return total


def _process_export_data_files(s3_client, s3_bucket, table_name, data_files, max_workers,
                                context=None, start_index=0):
    """
    Parse and write each export data file from start_index onward, returning aggregate
    counts plus (next_index, interrupted) so an interrupted run can resume at the next
    unprocessed file instead of re-writing files already written (writes are upserts,
    so redoing a file is harmless, but resuming avoids the wasted work).
    """
    total_items_processed = 0
    total_items_written = 0
    total_items_failed = 0
    failed_files = []

    for i in range(start_index, len(data_files)):
        data_file = data_files[i]
        logger.info(f" Processing file {i + 1}/{len(data_files)}: {data_file}")

        items, file_failed = parse_dynamodb_json_file(s3_client, s3_bucket, data_file)
        if file_failed:
            logger.error(f"File failed to parse and was skipped entirely: {data_file}")
            failed_files.append(data_file)
        elif not items:
            logger.warning(f"No items found in {data_file}")
        else:
            logger.info(f"Found {len(items)} items in {data_file}")

            written, failed = batch_write_items_to_table(table_name, items, max_workers)

            total_items_processed += len(items)
            total_items_written += written
            total_items_failed += failed

        is_last_file = i == len(data_files) - 1
        if not is_last_file and _time_running_low(context):
            logger.warning(f"Pausing write for {table_name} after file {i + 1}/{len(data_files)} "
                            f"to avoid hitting the Lambda timeout mid-restore")
            return total_items_processed, total_items_written, total_items_failed, failed_files, i + 1, True

        # Brief pause between files to avoid overwhelming DynamoDB
        if not is_last_file:
            time.sleep(1)

    return total_items_processed, total_items_written, total_items_failed, failed_files, len(data_files), False


def _determine_restore_status(failed_files, total_items_failed, total_items_written, expected_items):
    if expected_items > 0 and total_items_written == 0:
        return 'FAILED'
    if failed_files:
        return 'PARTIAL_SUCCESS' if total_items_written > 0 else 'FAILED'
    if total_items_failed == 0:
        return 'COMPLETED'
    if total_items_written > 0:
        return 'PARTIAL_SUCCESS'
    return 'FAILED'


def _build_restore_warning(result, failed_files, total_items_processed, total_items_failed, expected_items):
    if failed_files:
        result['failed_files'] = failed_files
        result['warning'] = f"{len(failed_files)} file(s) failed to parse and were skipped entirely"
    elif total_items_processed != expected_items:
        result['count_mismatch'] = f"Parsed {total_items_processed} items but export reported {expected_items}"

    if total_items_failed > 0:
        existing_warning = result.get('warning')
        failed_write_warning = f"{total_items_failed} items failed to write"
        result['warning'] = f"{existing_warning}; {failed_write_warning}" if existing_warning else failed_write_warning


def _verify_restored_count(result, table_name, expected_items, cleared):
    try:
        actual_count = count_table_items(table_name)
        result['actual_table_count'] = actual_count

        if not cleared:
            # Without clear_existing, the restore merges into whatever was
            # already in the table, so the table count legitimately exceeds
            # expected_items - comparing against it would be misleading.
            logger.info("Skipping count comparison: restore merged into existing data")
        elif actual_count != expected_items:
            result['count_mismatch'] = (
                f"Table has {actual_count} items but export reported {expected_items} "
                f"(diff: {actual_count - expected_items})"
            )
    except Exception as e:
        logger.exception(f"Post-restore count verification failed for {table_name}")
        result['count_verification_error'] = str(e)


def _build_interrupted_result(table_name, phase, **resume_fields):
    """Marks a table's restore as paused partway through so the orchestrator can
    self-invoke a continuation instead of treating it as a normal outcome."""
    return {
        'table_name': table_name,
        'restore_type': 'BATCH_WRITE_FROM_S3',
        'status': 'INTERRUPTED',
        'resume_state': {'phase': phase, **resume_fields}
    }


def restore_table_from_s3_export(s3_client, table_name, export_info, s3_bucket, clear_existing=False,
                                  max_workers=5, verify_count=False, context=None, resume_state=None):
    """
    Restore table data from S3 export using batch write operations.

    context/resume_state let this pick up mid-restore: if get_remaining_time_in_millis()
    runs low, this returns an INTERRUPTED result with enough state (scan cursor or next
    file index) for the caller to re-invoke and continue rather than being killed
    mid-write by the Lambda timeout.
    """
    logger.info(f" Starting batch write restore for table: {table_name}")
    resume_state = resume_state or {}

    try:
        phase = resume_state.get('phase', 'CLEARING' if clear_existing else 'WRITING')
        items_cleared = resume_state.get('items_cleared', 0)

        if clear_existing and phase == 'CLEARING':
            logger.warning(" Clearing existing data as requested")
            success, deleted, last_evaluated_key, interrupted = clear_existing_table_data(
                table_name, context=context, start_key=resume_state.get('last_evaluated_key')
            )
            items_cleared += deleted
            if not success:
                raise RuntimeError("Failed to clear existing table data")
            if interrupted:
                return _build_interrupted_result(
                    table_name, 'CLEARING', items_cleared=items_cleared, last_evaluated_key=last_evaluated_key
                )
            phase = 'WRITING'

        data_files = get_export_data_files(s3_client, s3_bucket, export_info)
        if not data_files:
            raise RuntimeError("No data files found for export")

        start_index = resume_state.get('file_index', 0) if phase == 'WRITING' else 0
        prior_processed = resume_state.get('items_processed', 0)
        prior_written = resume_state.get('items_written', 0)
        prior_failed = resume_state.get('items_failed', 0)
        prior_failed_files = resume_state.get('failed_files', [])

        processed, written, failed, new_failed_files, next_index, interrupted = _process_export_data_files(
            s3_client, s3_bucket, table_name, data_files, max_workers, context=context, start_index=start_index
        )

        total_items_processed = prior_processed + processed
        total_items_written = prior_written + written
        total_items_failed = prior_failed + failed
        failed_files = prior_failed_files + new_failed_files

        if interrupted:
            return _build_interrupted_result(
                table_name, 'WRITING', items_cleared=items_cleared, file_index=next_index,
                items_processed=total_items_processed, items_written=total_items_written,
                items_failed=total_items_failed, failed_files=failed_files
            )

        expected_items = export_info.get('item_count', 0)

        # Success rate against the expected export count, not just the items we
        # managed to parse - a file that failed to parse would otherwise still
        # show 100% success.
        success_rate = (total_items_written / expected_items * 100) if expected_items > 0 else 0
        status = _determine_restore_status(failed_files, total_items_failed, total_items_written, expected_items)

        result = {
            'table_name': table_name,
            'restore_type': 'BATCH_WRITE_FROM_S3',
            'status': status,
            'total_files_processed': len(data_files),
            'items_cleared': items_cleared,
            'items_processed': total_items_processed,
            'items_written': total_items_written,
            'items_failed': total_items_failed,
            'success_rate': f"{success_rate:.2f}%",
            'expected_items': expected_items,
            'export_arn': export_info.get('export_arn', 'unknown')
        }

        _build_restore_warning(result, failed_files, total_items_processed, total_items_failed, expected_items)

        if verify_count:
            _verify_restored_count(result, table_name, expected_items, clear_existing)

        logger.info(f" Batch write restore completed for {table_name}")
        logger.info(f"Results: {total_items_written}/{expected_items} items written, {success_rate:.2f} percent success")

        return result

    except Exception as e:
        logger.exception(f" Batch write restore failed for {table_name}")
        return {
            'table_name': table_name,
            'restore_type': 'BATCH_WRITE_FROM_S3',
            'status': 'FAILED',
            'error': str(e)
        }


def _parse_restore_request(event):
    return {
        'restore_mode': event.get('mode', 's3').lower(),
        'backup_date': event.get('backup_date', 'latest'),
        'specific_tables': event.get('tables', []),
        'dry_run': event.get('dry_run', False),
        'clear_existing_data': event.get('clear_existing_data', False),
        'max_workers': event.get('max_workers', 5),
        'verify_count': event.get('verify_count', False),
    }


def _resolve_s3_bucket(restore_mode, env_config):
    if restore_mode != 'b2':
        return env_config['backup_bucket']

    s3_bucket = os.environ.get('B2_BUCKET_NAME')
    if not s3_bucket:
        raise ValueError("B2 mode requires B2_BUCKET_NAME environment variable")
    return s3_bucket


def _log_restore_configuration(config, environment, s3_bucket, s3_prefix):
    logger.info("  Configuration:")
    logger.info(f"  Environment: {environment}")
    logger.info(f"  Storage Mode: {config['restore_mode'].upper()}")
    logger.info(f"  Bucket: {s3_bucket}")
    logger.info(f"  Prefix: {s3_prefix}")
    logger.info(f"  Backup Date: {config['backup_date']}")
    logger.info(f"  Specific Tables: {config['specific_tables'] or 'All available'}")
    logger.info(f"  Dry Run: {config['dry_run']}")
    logger.info(f"  Clear Existing Data: {config['clear_existing_data']}")
    logger.info(f"  Max Workers: {config['max_workers']}")
    logger.info(f"  Verify Count: {config['verify_count']}")

    if config['clear_existing_data']:
        logger.warning(" WARNING: clear_existing_data=True will DELETE ALL existing data before restore!")


def _resolve_tables_to_restore(specific_tables, all_available_tables):
    if not specific_tables:
        return all_available_tables

    invalid_tables = [t for t in specific_tables if t not in all_available_tables]
    if invalid_tables:
        logger.warning(f"Invalid tables requested: {invalid_tables}")

    tables_to_restore = [table for table in specific_tables if table in all_available_tables]
    if not tables_to_restore:
        raise ValueError(f"None of the specified tables are available. Available: {all_available_tables}")
    return tables_to_restore


def _resolve_backup_date(s3_client, s3_bucket, s3_prefix, backup_date):
    if backup_date != 'latest':
        return backup_date

    available_backups = get_available_backups(s3_client, s3_bucket, s3_prefix)
    if not available_backups:
        raise RuntimeError(f"No backups found in {s3_bucket}/{s3_prefix}/")

    resolved_date = available_backups[0]
    logger.info(f" Using latest backup from: {resolved_date}")
    return resolved_date


def _build_available_exports(manifest, tables_to_restore, backup_date):
    available_exports = {}
    invalid_exports = []

    for export in manifest.get('exports', []):
        try:
            validate_export_info(export)
            table_name = export['table_name']
            if table_name in tables_to_restore:
                available_exports[table_name] = export
        except ValueError as e:
            invalid_exports.append(f"{export.get('table_name', 'unknown')}: {str(e)}")

    if invalid_exports:
        logger.warning(f" Invalid exports found: {invalid_exports}")

    logger.info(f"Available exports for {backup_date}: {list(available_exports.keys())}")

    missing_exports = [t for t in tables_to_restore if t not in available_exports]
    if missing_exports:
        logger.warning(f" No valid exports found for tables: {missing_exports}")

    if not available_exports:
        raise RuntimeError("No valid exports found for any requested tables")

    return available_exports


def _estimate_export_size_mb(s3_client, s3_bucket, data_files):
    total_file_size = 0
    for file_key in data_files[:5]:  # Sample first 5 files
        try:
            response = s3_client.head_object(Bucket=s3_bucket, Key=file_key)
            total_file_size += response['ContentLength']
        except Exception:
            logger.debug("Failed to head_object %s for size estimate", file_key, exc_info=True)
    return round(total_file_size / 1024 / 1024, 2)


def _validate_table_for_dry_run(s3_client, s3_bucket, table_name, export_info, config):
    try:
        data_files = get_export_data_files(s3_client, s3_bucket, export_info)
        return {
            'table_name': table_name,
            'status': 'READY',
            'export_arn': export_info.get('export_arn', 'unknown'),
            'expected_items': export_info.get('item_count', 0),
            'data_files_count': len(data_files),
            'estimated_size_mb': _estimate_export_size_mb(s3_client, s3_bucket, data_files),
            's3_prefix_used': export_info.get('s3_prefix', 'unknown'),
            'restore_options': {
                'clear_existing_data': config['clear_existing_data'],
                'max_workers': config['max_workers']
            }
        }
    except Exception as e:
        return {'table_name': table_name, 'status': 'ERROR', 'error': str(e)}


def _run_dry_run(s3_client, s3_bucket, s3_prefix, environment, backup_date, tables_to_restore,
                  available_exports, config):
    logger.info(" DRY RUN MODE - Validating restore capability without writing data")

    validation_results = []
    for table_name in tables_to_restore:
        if table_name not in available_exports:
            validation_results.append({
                'table_name': table_name,
                'status': 'NO_EXPORT',
                'error': 'No valid export found for this table'
            })
            continue

        validation_results.append(
            _validate_table_for_dry_run(s3_client, s3_bucket, table_name, available_exports[table_name], config)
        )

    dry_run_summary = {
        'dry_run': True,
        'backup_date': backup_date,
        'environment': environment,
        's3_bucket': s3_bucket,
        's3_prefix': s3_prefix,
        'restore_type': 'BATCH_WRITE_FROM_S3_TO_EXISTING_TABLES',
        'tables_requested': len(tables_to_restore),
        'validation_results': validation_results,
        'configuration': {
            'clear_existing_data': config['clear_existing_data'],
            'max_workers': config['max_workers']
        },
        'warnings': [
            'This approach writes directly to existing tables with the same names',
            'Set clear_existing_data=true to clear existing data first',
            'Restore will merge with existing data if clear_existing_data=false'
        ]
    }

    return {
        'statusCode': 200,
        'body': json.dumps(dry_run_summary, default=decimal_default, indent=2)
    }


def _run_table_restores(s3_client, s3_bucket, tables_to_restore, available_exports, config,
                         context=None, restore_results=None, resume_table_state=None):
    """
    Restore each table in order. Returns (restore_results, remaining_tables, resume_table_state).
    remaining_tables is empty when every table finished; otherwise the first entry is the
    table that was in progress (paired with resume_table_state) and the rest haven't started.
    """
    logger.info(f" Starting batch write restore for {len(available_exports)} tables")
    restore_results = list(restore_results) if restore_results else []

    for i, table_name in enumerate(tables_to_restore):
        if _time_running_low(context):
            logger.warning(f"Pausing restore before starting {table_name} to avoid the Lambda timeout")
            return restore_results, tables_to_restore[i:], None

        if table_name not in available_exports:
            logger.warning(f"Skipping {table_name} - no valid export found")
            restore_results.append({
                'table_name': table_name,
                'restore_type': 'BATCH_WRITE_FROM_S3',
                'status': 'SKIPPED',
                'error': 'No valid export found for this table'
            })
            continue

        logger.info(f" Starting restore for {table_name}")
        result = restore_table_from_s3_export(
            s3_client,
            table_name,
            available_exports[table_name],
            s3_bucket,
            clear_existing=config['clear_existing_data'],
            max_workers=config['max_workers'],
            verify_count=config['verify_count'],
            context=context,
            resume_state=resume_table_state if i == 0 else None
        )
        resume_table_state = None  # only ever applies to the (possibly resumed) first table

        if result.get('status') == 'INTERRUPTED':
            return restore_results, tables_to_restore[i:], result['resume_state']

        restore_results.append(result)

        # Brief pause between tables
        if len(available_exports) > 1:
            time.sleep(2)

    return restore_results, [], None


def _build_restore_summary(restore_results, start_time, environment, s3_bucket, s3_prefix,
                            backup_date, tables_to_restore, config):
    end_time = datetime.now()
    duration = end_time - start_time

    successful_restores = len([r for r in restore_results if r.get('status') == 'COMPLETED'])
    partial_restores = len([r for r in restore_results if r.get('status') == 'PARTIAL_SUCCESS'])
    failed_restores = len([r for r in restore_results if r.get('status') == 'FAILED'])
    skipped_restores = len([r for r in restore_results if r.get('status') == 'SKIPPED'])

    total_items_written = sum(r.get('items_written', 0) for r in restore_results)
    total_items_processed = sum(r.get('items_processed', 0) for r in restore_results)

    summary = {
        'backup_date': backup_date,
        'environment': environment,
        's3_bucket': s3_bucket,
        's3_prefix': s3_prefix,
        'restore_type': 'BATCH_WRITE_FROM_S3_TO_EXISTING_TABLES',
        'duration_seconds': int(duration.total_seconds()),
        'tables_requested': len(tables_to_restore),
        'successful_restores': successful_restores,
        'partial_restores': partial_restores,
        'failed_restores': failed_restores,
        'skipped_restores': skipped_restores,
        'total_items_written': total_items_written,
        'total_items_processed': total_items_processed,
        'configuration': {
            'clear_existing_data': config['clear_existing_data'],
            'max_workers': config['max_workers']
        },
        'restore_results': restore_results,
        'completed_at': end_time.isoformat()
    }

    logger.info(f"Batch write restore completed in {duration}")
    logger.info(
        f"Results: {successful_restores} completed, {partial_restores} partial, "
        f"{failed_restores} failed, {skipped_restores} skipped"
    )
    logger.info(f"Total items: {total_items_written}/{total_items_processed} written")

    if failed_restores > 0 and successful_restores == 0:
        status_code = 500
    elif failed_restores > 0 or partial_restores > 0:
        status_code = 207  # Multi-status
    else:
        status_code = 200

    return {
        'statusCode': status_code,
        'body': json.dumps(summary, default=decimal_default, indent=2)
    }


def _build_error_response(error, start_time):
    end_time = datetime.now()
    duration = end_time - start_time

    logger.exception(f"Critical error in batch write restore after {duration}")

    error_response = {
        'error': str(error),
        'restore_type': 'BATCH_WRITE_FROM_S3',
        'environment': os.environ.get('ENVIRONMENT', 'unknown'),
        's3_bucket': os.environ.get('BACKUP_BUCKET', 'unknown'),
        's3_prefix': os.environ.get('S3_EXPORTS_PREFIX', 'native-exports'),
        'duration_seconds': int(duration.total_seconds()),
        'failed_at': end_time.isoformat()
    }

    return {
        'statusCode': 500,
        'body': json.dumps(error_response, default=decimal_default, indent=2)
    }


def _trigger_restore_continuation(function_name, backup_date, config, remaining_tables, resume_table_state,
                                   restore_results, tables_requested, start_time, continuation_count):
    """Asynchronously re-invoke this Lambda to keep restoring where this invocation left off"""
    payload = {
        'continue_restore': True,
        'backup_date': backup_date,
        'restore_mode': config['restore_mode'],
        'clear_existing_data': config['clear_existing_data'],
        'max_workers': config['max_workers'],
        'verify_count': config['verify_count'],
        'remaining_tables': remaining_tables,
        'resume_table_state': resume_table_state,
        'restore_results': restore_results,
        'tables_requested': tables_requested,
        'start_time': start_time.isoformat(),
        'continuation_count': continuation_count,
    }

    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(payload, default=decimal_default).encode('utf-8')
        )
        logger.warning(f"Restore continuation {continuation_count} triggered for {len(remaining_tables)} "
                        f"remaining table(s): {remaining_tables}")
    except Exception:
        logger.exception("Failed to trigger restore continuation")


def _handle_restore_interruption(function_name, restore_results, remaining_tables, resume_table_state,
                                  start_time, environment, s3_bucket, s3_prefix, backup_date,
                                  tables_requested, config, continuation_count):
    """
    Called when _run_table_restores stopped early because the Lambda was running out of
    time. Either self-invokes a continuation, or - if MAX_RESTORE_CONTINUATIONS has been
    hit - gives up and marks whatever's left FAILED so it's visible rather than silently
    incomplete.
    """
    if continuation_count > MAX_RESTORE_CONTINUATIONS:
        logger.error(f"Restore for {backup_date} did not finish after {continuation_count - 1} continuations; "
                     f"giving up on: {remaining_tables}")
        for table_name in remaining_tables:
            restore_results.append({
                'table_name': table_name,
                'restore_type': 'BATCH_WRITE_FROM_S3',
                'status': 'FAILED',
                'error': f"Restore did not complete after {continuation_count - 1} continuation attempts"
            })
        return _build_restore_summary(
            restore_results, start_time, environment, s3_bucket, s3_prefix,
            backup_date, tables_requested, config
        )

    _trigger_restore_continuation(
        function_name, backup_date, config, remaining_tables, resume_table_state,
        restore_results, tables_requested, start_time, continuation_count
    )

    return {
        'statusCode': 202,
        'body': json.dumps({
            'status': 'IN_PROGRESS',
            'backup_date': backup_date,
            'completed_tables': [r['table_name'] for r in restore_results],
            'remaining_tables': remaining_tables,
            'continuation_count': continuation_count,
            'message': 'Restore paused before the Lambda time limit; continuing asynchronously.'
        }, default=decimal_default)
    }


def _continue_restore(event, context):
    """Resume a restore that a previous invocation paused via _handle_restore_interruption"""
    backup_date = event['backup_date']
    config = {
        'restore_mode': event['restore_mode'],
        'clear_existing_data': event['clear_existing_data'],
        'max_workers': event['max_workers'],
        'verify_count': event['verify_count'],
    }
    remaining_tables = event['remaining_tables']
    resume_table_state = event.get('resume_table_state')
    restore_results = event.get('restore_results', [])
    tables_requested = event.get('tables_requested', remaining_tables)
    start_time = datetime.fromisoformat(event['start_time'])
    continuation_count = event.get('continuation_count', 1)

    logger.info(f"Resuming restore for backup_date={backup_date}, continuation {continuation_count}, "
                f"{len(remaining_tables)} table(s) remaining")

    try:
        env_config = validate_environment()
        environment = env_config['environment']
        s3_prefix = env_config['s3_prefix']

        s3_client = get_storage_client(config['restore_mode'])
        s3_bucket = _resolve_s3_bucket(config['restore_mode'], env_config)

        manifest = get_backup_manifest(s3_client, s3_bucket, backup_date, s3_prefix)
        if not manifest:
            raise RuntimeError(f"Could not find or parse backup manifest for {backup_date}")

        available_exports = _build_available_exports(manifest, remaining_tables, backup_date)

        new_results, still_remaining, next_resume_state = _run_table_restores(
            s3_client, s3_bucket, remaining_tables, available_exports, config, context=context,
            restore_results=restore_results, resume_table_state=resume_table_state
        )

        if still_remaining:
            return _handle_restore_interruption(
                context.function_name, new_results, still_remaining, next_resume_state,
                start_time, environment, s3_bucket, s3_prefix, backup_date,
                tables_requested, config, continuation_count + 1
            )

        return _build_restore_summary(
            new_results, start_time, environment, s3_bucket, s3_prefix,
            backup_date, tables_requested, config
        )

    except Exception as e:
        return _build_error_response(e, start_time)


def lambda_handler(event, context):
    """
    Main handler for batch write restoration from Backup exports in S3 or B2
    """
    if event.get('continue_restore'):
        return _continue_restore(event, context)

    start_time = datetime.now()

    config = _parse_restore_request(event)
    storage_type = 'B2' if config['restore_mode'] == 'b2' else 'S3'

    logger.info(f" Starting MFA Dynamodb restore from {storage_type} exports at {start_time}")

    try:
        env_config = validate_environment()
        environment = env_config['environment']
        s3_prefix = env_config['s3_prefix']  # Will be 'native-exports' by default

        s3_client = get_storage_client(config['restore_mode'])
        s3_bucket = _resolve_s3_bucket(config['restore_mode'], env_config)

        _log_restore_configuration(config, environment, s3_bucket, s3_prefix)

        all_available_tables = get_tables_to_restore()
        tables_to_restore = _resolve_tables_to_restore(config['specific_tables'], all_available_tables)
        logger.info(f" Tables to restore: {tables_to_restore}")

        backup_date = _resolve_backup_date(s3_client, s3_bucket, s3_prefix, config['backup_date'])

        manifest = get_backup_manifest(s3_client, s3_bucket, backup_date, s3_prefix)
        if not manifest:
            raise RuntimeError(f"Could not find or parse backup manifest for {backup_date}")

        available_exports = _build_available_exports(manifest, tables_to_restore, backup_date)

        if config['dry_run']:
            return _run_dry_run(
                s3_client, s3_bucket, s3_prefix, environment, backup_date,
                tables_to_restore, available_exports, config
            )

        restore_results, remaining_tables, resume_table_state = _run_table_restores(
            s3_client, s3_bucket, tables_to_restore, available_exports, config, context=context
        )

        if remaining_tables:
            return _handle_restore_interruption(
                context.function_name, restore_results, remaining_tables, resume_table_state,
                start_time, environment, s3_bucket, s3_prefix, backup_date,
                tables_to_restore, config, continuation_count=1
            )

        return _build_restore_summary(
            restore_results, start_time, environment, s3_bucket, s3_prefix,
            backup_date, tables_to_restore, config
        )

    except Exception as e:
        return _build_error_response(e, start_time)
