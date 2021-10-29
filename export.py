import sys
import argparse
from google.cloud import firestore, storage
from google.cloud.firestore_v1 import query
from joblib import Parallel, delayed
from worker import ExportConfig, Worker

parser = argparse.ArgumentParser(
    description='Export Firestore collection to GCS')
parser.add_argument(
    '--project',
    help='Firebase Project ID for project containing the Cloud Firestore database',
    required=True)
parser.add_argument(
    '--source-collection',
    help='The path of the the Cloud Firestore Collection to read from',
    required=True)
parser.add_argument(
    '--collection-group',
    action='store_true',
    help='Whether the source collection is a collection group')
parser.add_argument(
    '--dest-bucket',
    help='The GCS bucket to store the export files',
    required=True)
parser.add_argument(
    '--batch-size',
    help='The limit to use when reading from Firestore',
    default=500,
    type=int)
parser.add_argument(
    '--num-partitions',
    type=int,
    default=1,
    help='Number of partition to create to load the data; default=1 (for Collection Group only)')
parser.add_argument(
    '--num-threads',
    type=int,
    default=1,
    help='Number of thread to use, useful for collection group export; default=1 (for Collection Group only)')


if __name__ == '__main__':
    args = parser.parse_args()

    project = args.project
    collection = args.source_collection
    is_collection_group = args.collection_group
    bucket = args.dest_bucket
    batch_size = args.batch_size
    num_partitions = args.num_partitions
    num_threads = args.num_threads

    if is_collection_group and '/' in collection:
        print('[ERR] Collection group must not contain "/"')
        sys.exit(1)

    db = firestore.Client(project=project)
    gcs = storage.Client(project=project)
    config = ExportConfig(project=project,
                          source_collection=collection,
                          dest_bucket=bucket,
                          batch_size=batch_size,
                          is_collection_group=is_collection_group)

    # if multi-threading and partitioning specified, ignore the batch size since the
    # batch size we'll just load all documents inside that partition.
    if is_collection_group and num_partitions > 1 and num_threads > 1:
        jobs = []
        partitions = db \
            .collection_group(collection) \
            .get_partitions(num_partitions)
        counter = 1
        while True:
            try:
                partition = next(partitions)
                updated_config = config.set(query_partition=partition)
                jobs.append(delayed(Worker.create)(
                    f'part-{counter}', db, gcs, updated_config))
                counter += 1
            except StopIteration:
                break

        Parallel(n_jobs=num_threads, prefer='threads', verbose=1)(jobs)

    # else, process them normally.
    else:
        Worker.create('part-1', db, gcs, config)
