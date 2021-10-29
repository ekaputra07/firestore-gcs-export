import os
import sys
import json
import jsonlines
from dataclasses import dataclass, replace
from tempfile import NamedTemporaryFile
from datetime import datetime
from google.cloud import firestore, storage
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.cloud.firestore_v1.base_query import QueryPartition


@dataclass
class ExportConfig:
    project: str
    source_collection: str
    dest_bucket: str
    batch_size: int = 500
    is_collection_group: bool = False
    query_partition: QueryPartition = None

    def set(self, **kwargs):
        return replace(self, **kwargs)

    @property
    def cleaned_source_collection(self):
        collection = self.source_collection
        if self.source_collection.startswith('/'):
            return collection[1:]
        return collection

    @property
    def snaked_source_collection(self):
        return self.cleaned_source_collection \
            .replace('/', '_') \
            .replace('-', '_').lower()


class Worker:
    IMPORT_TS = datetime(1970, 1, 1).isoformat()
    IMPORT_OPERATION = 'IMPORT'
    IMPORT_EVENT_ID = ''

    def __init__(self,
                 id: str,
                 firestore_client: firestore.Client,
                 gcs_client: storage.Client,
                 export_config: ExportConfig,
                 workspace: str):
        self.id = id
        self.firestore_client = firestore_client
        self.gcs_client = gcs_client
        self.workspace = workspace
        self.export_config = export_config
        self.cursor_file = os.path.join(
            workspace, f'{export_config.snaked_source_collection}.cursor')

    def to_items(self, result):
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
                doc_name = os.path.join(
                    f'projects/{self.export_config.project}/databases/(default)/documents/', doc_path)
                item = {
                    'timestamp': self.IMPORT_TS,
                    'event_id': self.IMPORT_EVENT_ID,
                    'document_name': doc_name,
                    'operation': self.IMPORT_OPERATION,
                    'data': json.dumps(doc.to_dict(), default=default),
                    'document_id': doc.id,
                }
                items.append(item)
        return items, first_doc, last_doc

    def upload_items_to_gcs(self, items, object_path):
        bucket_name = self.export_config.dest_bucket
        if items:
            # create temp file to store the data
            f = NamedTemporaryFile(
                delete=False,
                suffix=object_path.replace('/', '-')
            )
            filename = f.name

            # write jsonlines
            with jsonlines.open(filename, mode='w') as writer:
                writer.write_all(items)

            # upload to GCS
            bucket = self.gcs_client.get_bucket(bucket_name)
            blob = bucket.blob(object_path)
            blob.upload_from_filename(filename=filename)

            # delete temp file
            os.unlink(filename)
            print(
                f'[OK] {len(items)} rows uploaded to gs://{bucket_name}/{object_path}')

    def query(self, cursor=None):
        is_collection_group = self.export_config.is_collection_group
        source_collection = self.export_config.cleaned_source_collection
        batch_size = self.export_config.batch_size

        if is_collection_group:
            q = self.firestore_client.collection_group(source_collection)
        else:
            q = self.firestore_client.collection(source_collection)

        if cursor:
            q = q.start_after(cursor)

        result = q.limit(batch_size).get()
        return self.to_items(result)

    def create_gcs_object_path(self, first_doc, last_doc):
        return f'firestore_{self.export_config.snaked_source_collection}_export/{self.id}-{first_doc.id}-to-{last_doc.id}.json'

    def read_cursor(self):
        try:
            with open(self.cursor_file, 'r') as file:
                return file.readline()
        except FileNotFoundError:
            return None

    def write_cursor(self, id: str):
        os.makedirs(os.path.dirname(self.cursor_file), exist_ok=True)
        with open(self.cursor_file, 'w') as file:
            file.write(id)

    def start_in_partition(self):
        query = self.export_config.query_partition.query()
        result = query.get()
        items, first, last = self.to_items(result)
        object_path = self.create_gcs_object_path(first, last)
        self.upload_items_to_gcs(
            items=items,
            object_path=object_path)

    def start(self):
        cursor_id = self.read_cursor()
        cursor = None

        if cursor_id:
            source_collection = self.export_config.cleaned_source_collection
            cursor = self.firestore_client \
                .collection(source_collection) \
                .document(cursor_id) \
                .get()
            if not cursor.exists:
                print(
                    f'[ERR] Document with id={cursor_id} does not exists.')
                sys.exit(1)

        while True:
            items, first, last = self.query(cursor=cursor)
            cursor = last
            if not cursor:
                break
            object_path = self.create_gcs_object_path(first, last)
            self.upload_items_to_gcs(
                items=items,
                object_path=object_path)
            self.write_cursor(last.id)

    @staticmethod
    def create(
            id: int,
            firestore_client: firestore.Client,
            gcs_client: storage.Client,
            export_config: ExportConfig,
            workspace='workspace/'):
        worker = Worker(id, firestore_client, gcs_client,
                        export_config, workspace)
        if export_config.query_partition:
            worker.start_in_partition()
        else:
            worker.start()
