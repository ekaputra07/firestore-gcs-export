import os
import sys
import json
import jsonlines
from dataclasses import dataclass, replace
from tempfile import NamedTemporaryFile
from datetime import datetime
from google.cloud import firestore, storage
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

firestore_client = firestore.Client()
gcs_client = storage.Client()


@dataclass
class ExportConfig:
    """
    Configuration that specific to export job that
    we need to pass to the worker.
    """
    project: str
    source_collection: str
    dest_bucket: str
    batch_size: int = 500
    is_collection_group: bool = False
    partition_config_file: str = None

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
    """
    The actual code that doing the export job.
    """
    IMPORT_TS = datetime(1970, 1, 1).isoformat()
    IMPORT_OPERATION = 'IMPORT'
    IMPORT_EVENT_ID = ''

    def __init__(self,
                 export_config: ExportConfig,
                 workspace: str):
        self.workspace = workspace
        self.export_config = export_config
        self.cursor_file = os.path.join(
            workspace, f'{export_config.snaked_source_collection}.cursor')

    def to_items(self, result):
        """
        Convert query result into dictionary that will match
        the output of firebase-bigquery-export extension.
        """
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
        """
        Upload jsonlines file into GCS.
        """
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
            bucket = gcs_client.get_bucket(bucket_name)
            blob = bucket.blob(object_path)
            blob.upload_from_filename(filename=filename)

            # delete temp file
            os.unlink(filename)
            print(
                f'[OK] {len(items)} rows uploaded to gs://{bucket_name}/{object_path}')

    def get_document(self, path):
        doc = firestore_client.document(path).get()
        return doc if doc.exists else None

    def query_collection(self, cursor=None):
        source_collection = self.export_config.cleaned_source_collection
        batch_size = self.export_config.batch_size

        q = firestore_client \
            .collection(source_collection) \
            .limit(batch_size)

        if cursor:
            q = q.start_after(cursor)
        return self.to_items(q.get())

    def query_collection_group(self, start_at_path=None, end_at_path=None):
        group = self.export_config.cleaned_source_collection
        q = firestore_client.collection_group(group)

        if start_at_path:
            # TODO: can we avoid this and create DocumentSnapshot from path?
            start_doc = self.get_document(start_at_path)
            if not start_doc:
                raise Exception(
                    f'Cursor document {start_at_path} does not exists!')
            # partition start_at == previous partition end_at so here we use start_after
            # to avoid duplicates.
            q = q.start_after(start_doc)

        if end_at_path:
            # TODO: can we avoid this and create DocumentSnapshot from path?
            end_doc = self.get_document(end_at_path)
            if not end_doc:
                raise Exception(
                    f'Cursor document {end_at_path} does not exists!')
            q = q.end_at(end_doc)
        return self.to_items(q.get())

    def create_gcs_object_path(self, first_doc, last_doc, prefix=''):
        return f'firestore_{self.export_config.snaked_source_collection}_export/{prefix}{first_doc.id}-to-{last_doc.id}.json'

    def read_cursor_id(self):
        try:
            with open(self.cursor_file, 'r') as file:
                return file.readline().strip()
        except FileNotFoundError:
            return None

    def write_cursor_id(self, id: str):
        os.makedirs(os.path.dirname(self.cursor_file), exist_ok=True)
        with open(self.cursor_file, 'w') as file:
            file.write(id)

    def export_collection_group(self):
        """
        We're exporting per partition based on partition config file path given
        in export config.
        """
        path = self.export_config.partition_config_file
        config = {}
        try:
            with open(path, 'r') as file:
                config = json.load(file)

            partition_num = config.get('partition_num', 0)
            start_at_path = config.get('start_at_path')
            end_at_path = config.get('end_at_path')

            items, first, last = self.query_collection_group(
                start_at_path=start_at_path, end_at_path=end_at_path)
            object_path = self.create_gcs_object_path(
                first, last, prefix=f'part-{partition_num}-')
            self.upload_items_to_gcs(
                items=items,
                object_path=object_path)

            # success, delete the partition config file
            os.unlink(path)
        except Exception as e:
            print(f'[ERR] Failed to export partition {partition_num}: {e}')

    def export_collection(self):
        cursor_id = self.read_cursor_id()
        cursor = None
        if cursor_id:
            cursor = self.get_document(
                f'{self.export_config.cleaned_source_collection}/{cursor_id}')
            if not cursor:
                print(
                    f'[ERR] Document with id={cursor_id} does not exists!')
                sys.exit(1)

        while True:
            try:
                items, first, last = self.query_collection(cursor=cursor)
                cursor = last
                if not cursor:
                    break
                object_path = self.create_gcs_object_path(first, last)
                self.upload_items_to_gcs(
                    items=items,
                    object_path=object_path)
                self.write_cursor_id(last.id)
            except Exception as e:
                print(f'[ERR] Error occured during export: {e}')
                break

    @staticmethod
    def create(export_config: ExportConfig, workspace: str):
        worker = Worker(export_config, workspace)
        if export_config.is_collection_group:
            worker.export_collection_group()
        else:
            worker.export_collection()
