import os
import sys
import json
import jsonlines
import argparse
from tempfile import NamedTemporaryFile
from datetime import datetime
from google.cloud import firestore, storage
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

IMPORT_TS = datetime(1970, 1, 1)
IMPORT_OPERATION = 'IMPORT'
IMPORT_EVENT_ID = ''


def to_items(project, result):
    items = []
    first_doc = None
    last_doc = None

    def default(obj):
        if isinstance(obj, DatetimeWithNanoseconds) and obj.nanosecond:
            return {
                '_seconds': int(obj.timestamp()),
                '_nanoseconds': int(obj.nanosecond)
            }
        if isinstance(obj, datetime):
            return {
                '_seconds': int(obj.timestamp()),
                '_nanoseconds': int(obj.microsecond) * 1000
            }
        raise TypeError(f'Not a datetime or DatetimeWithNanoseconds')

    if result:
        first_doc = result[0]
        last_doc = result[-1]
        for doc in result:
            doc_path = doc.reference.path
            doc_name = f'projects/{project}/databases/(default)/documents/{doc_path}'
            item = {
                'timestamp': IMPORT_TS.isoformat(),
                'event_id': IMPORT_EVENT_ID,
                'document_name': doc_name,
                'operation': IMPORT_OPERATION,
                'data': json.dumps(doc.to_dict(), default=default),
                'document_id': doc.id,
            }
            items.append(item)
    return items, first_doc, last_doc


def upload_items_to_gcs(client, items, bucket_name, object_path):
    if items:
        # create temp file to store the data
        f = NamedTemporaryFile(delete=False)
        filename = f.name

        # write jsonlines
        with jsonlines.open(filename, mode='w') as writer:
            writer.write_all(items)

        # upload to GCS
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_path)
        blob.upload_from_filename(filename=filename)

        # delete temp file
        os.unlink(filename)
        print(
            f'[OK] {len(items)} rows uploaded to gs://{bucket_name}/{object_path}')


def query(client, collection, batch_size, last_doc=None):
    if last_doc:
        result = client.collection(collection) \
            .start_after(last_doc) \
            .limit(batch_size) \
            .get()
    else:
        result = client.collection(collection) \
            .limit(batch_size) \
            .get()
    return to_items(project=client.project, result=result)


def create_gcs_object_path(collection, first_doc, last_doc):
    collection = collection.replace('/', '-')
    return f'firestore_{collection}_export/{first_doc.id}-to-{last_doc.id}.json'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Export Firestore collection to GCS')
    parser.add_argument(
        '--project', help='Firebase Project ID for project containing the Cloud Firestore database.', required=True)
    parser.add_argument('--source-collection',
                        help='The path of the the Cloud Firestore Collection to read from.', required=True)
    parser.add_argument(
        '--dest-bucket', help='The GCS bucket to store the export files.', required=True)
    parser.add_argument(
        '--batch-size', help='The GCS bucket to store the export files.', default=500, type=int)
    parser.add_argument(
        '--start-after', help='Firestore document ID to use as a start_after cursor for the query.')
    args = parser.parse_args()

    project = args.project
    collection = args.source_collection
    bucket = args.dest_bucket
    batch_size = args.batch_size
    start_after = args.start_after

    db = firestore.Client(project=project)
    gcs = storage.Client(project=project)

    last_doc = None
    if start_after:
        last_doc = db.collection(collection).document(start_after).get()
        if not last_doc.exists:
            print(f'[ERR] Document with id={start_after} does not exists.')
            sys.exit(1)

    # first call
    items, first, last = query(
        client=db,
        collection=collection,
        batch_size=batch_size,
        last_doc=last_doc)
    last_doc = last if last else None
    if last_doc:
        upload_items_to_gcs(client=gcs,
                            items=items,
                            bucket_name=bucket,
                            object_path=create_gcs_object_path(collection, first, last))

    while last_doc is not None:
        items, first, last = query(
            client=db,
            collection=collection,
            batch_size=batch_size,
            last_doc=last_doc)
        last_doc = last if last else None
        if last_doc:
            upload_items_to_gcs(client=gcs,
                                items=items,
                                bucket_name=bucket,
                                object_path=create_gcs_object_path(collection, first, last))
