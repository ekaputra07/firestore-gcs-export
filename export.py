import os
import sys
import json
import jsonlines
from tempfile import NamedTemporaryFile
from datetime import datetime
from google.cloud import firestore, storage
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from dotenv import load_dotenv

load_dotenv()

IMPORT_TS = datetime(1970, 1, 1)
IMPORT_OPERATION = 'IMPORT'
IMPORT_EVENT_ID = ''

PROJECT_ID = os.getenv('PROJECT_ID')
COLLECTION = os.getenv('COLLECTION')
GCS_BUCKET = os.getenv('GCS_BUCKET')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', default=500))

if not PROJECT_ID or not COLLECTION or not GCS_BUCKET:
    print('[ERROR] missing PROJECT_ID, COLLECTION or GCS_BUCKET!')
    sys.exit(1)

db = firestore.Client(project=PROJECT_ID)
gcs = storage.Client(project=PROJECT_ID)


def to_items(result):
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
            doc_name = f'projects/{PROJECT_ID}/databases/(default)/documents/{doc_path}'
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


def upload_items_to_gcs(items, bucket_name, object_path):
    if items:
        # create temp file to store the data
        f = NamedTemporaryFile(delete=False)
        filename = f.name

        # write jsonlines
        with jsonlines.open(filename, mode='w') as writer:
            writer.write_all(items)

        # upload to GCS
        bucket = gcs.get_bucket(bucket_name)
        blob = bucket.blob(object_path)
        blob.upload_from_filename(filename=filename)

        # delete temp file
        os.unlink(filename)
        print(
            f'[OK] {len(items)} rows uploaded to gs://{bucket_name}/{object_path}')


def query(collection, batch_size, last_doc=None):
    if last_doc:
        result = db.collection(collection) \
            .start_after(last_doc) \
            .limit(batch_size) \
            .get()
    else:
        result = db.collection(collection) \
            .limit(batch_size) \
            .get()
    return to_items(result)


def create_gcs_object_path(collection, first_doc, last_doc):
    collection = collection.replace('/', '-')
    return f'firestore_{collection}_export/{first_doc.id}-to-{last_doc.id}.json'


if __name__ == '__main__':
    last_doc = None

    # first call
    items, first, last = query(COLLECTION, BATCH_SIZE, last_doc=last_doc)
    last_doc = last if last else None
    if last_doc:
        upload_items_to_gcs(items,
                            GCS_BUCKET,
                            create_gcs_object_path(COLLECTION, first, last))

    while last_doc is not None:
        items, first, last = query(COLLECTION, BATCH_SIZE, last_doc=last_doc)
        last_doc = last if last else None
        if last_doc:
            upload_items_to_gcs(items,
                                GCS_BUCKET,
                                create_gcs_object_path(COLLECTION, first, last))
