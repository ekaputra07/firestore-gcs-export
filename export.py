import os
import sys
import argparse
import json
from google.cloud import firestore, storage
from joblib import Parallel, delayed
from worker import ExportConfig, Worker

WORKSPACE_DIR = 'workspace/'

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
    if is_collection_group:
        # get partitions
        partitions = db \
            .collection_group(collection) \
            .get_partitions(num_partitions)

        # create partition config files
        partition_dir = os.path.join(WORKSPACE_DIR, collection)
        try:
            # If partition dir not exists, start new export jobs
            os.makedirs(partition_dir)
            counter = 0
            while True:
                try:
                    partition = next(partitions)
                    counter += 1
                    with open(os.path.join(partition_dir, f'partition-{counter}.json'), 'w') as file:
                        conf = {
                            'partition_num': counter
                        }
                        if partition.start_at:
                            conf['start_at_path'] = partition.start_at.path
                        if partition.end_at:
                            conf['end_at_path'] = partition.end_at.path
                        json.dump(conf, file)
                except StopIteration:
                    break
            print(
                f'[INFO] {counter} partition export configs created under {partition_dir}')

        except FileExistsError:
            print(
                f'[INFO] Parition directory {partition_dir} exists, continuing unfinished jobs (if any).')

        print('[INFO] Export starting...')
        jobs = []

        # Read partition config files and run the export jobs
        for dirpath, dirnames, filenames in os.walk(partition_dir):
            for filename in filenames:
                config_file = os.path.join(dirpath, filename)
                updated_config = config.set(partition_config_file=config_file)
                jobs.append(delayed(Worker.create)(
                    db, gcs, updated_config, WORKSPACE_DIR))

        if jobs:
            Parallel(n_jobs=num_threads, prefer='threads', verbose=1)(jobs)
        else:
            print(
                f'[INFO] Do nothing. No partition config files found under {partition_dir}.')

    # else, process them normally.
    else:
        Worker.create(db, gcs, config, WORKSPACE_DIR)
