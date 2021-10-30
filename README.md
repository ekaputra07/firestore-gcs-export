# firestore-export-gcs

Simple utility to export Firebase collection to Cloud Storage.

The outputs are Jsonlines compatible with this extension: https://github.com/firebase/extensions/tree/master/firestore-bigquery-export

### Why do you create this while there's existing one that comes with the extension?

Because the one that comes with the extension somehow doesn't work anymore for me and I have no time to debug the cause. It was working previously but wasn't anymore, I created this quickly so that I can continue doing my job :)

> If the official one works for you than you might not need this.

### This tool export to GCS instead writing directly to Big Query

This is intentional, I feel it safer to have my data in GCS before I load it to Big Query. You can use it as backup tool as well.

### How it works

Similar to the official one, it basically will read all documents inside your Firestore collection and based on your batch size for each batch it will store them as a Jsonline document to Cloud Storage bucket that you need to specify.

In term of authentication, for the sake of simplicity it will using whichever GCP credential that currently active on your environment.

### How to use it

For now the easiest way is to clone this repo locally or in your server, install the requirements and run it.

Clone it:

```bash
git clone https://github.com/ekaputra07/firestore-gcs-export.git

# jump to the repo
cd firestore-gcs-export
```

Install dependencies:

```bash
pip3 install -r requirements.txt
```

### Exporting Collection

```bash
python3 export.py \
--project=my-project \
--dest-bucket=my-bucket \
--source-collection=posts
```

You will see something like this when it running:

```bash
[OK] 500 rows uploaded to gs://my-bucket/firestore_posts_export/doc1-to-doc500.json
[OK] 500 rows uploaded to gs://my-bucket/firestore_posts_export/doc501-to-doc1000.json
[OK] 500 rows uploaded to gs://my-bucket/firestore_posts_export/doc1001-to-doc1500.json
```

You can stop it using `CTRL+C` and start it again to continue because it keep track of the last document id that successfully exported.

Optional arguments:

```bash
--batch-size 1000 # default=500
```

### Exporting Collection Group

To export collection group this tool take advantage of partition that can be used to export multiple partitions in parallel by using multi-threading.

For small collection group you might not need it but I found it useful when the collection group is large.

```bash
python3 export.py \
--project=my-project \
--dest-bucket=my-bucket \
--source-collection=ratings \
--collection-group \
--num-partitions=1000 \ # default 1
--num-threads=4 # default 1
```

And you'll see something like this:

```bash
[INFO] 3 partition export configs created under workspace/ratings
[INFO] Export starting...

[Parallel(n_jobs=1)]: Using backend SequentialBackend with 1 concurrent workers.

[OK] 19 rows uploaded to gs://my-bucket/firestore_ratings_export/part-2-doc101-to-doc200.json
[OK] 61 rows uploaded to gs://my-bucket/firestore_ratings_export/part-1-doc1-to-doc100.json

[Parallel(n_jobs=1)]: Done   2 out of   2 | elapsed:    3.6s finished
```

> Please note that the number of partition created might not the same as you're expecting.

This tool keep track of the export job for each partition using a simple config file `workspace/{collection_group_name}/partition-*.json`.

Each export job/thread will read one file, once finish they will delete the file and in case of error they will keep the file so we will be able re-run the jobs again later.
