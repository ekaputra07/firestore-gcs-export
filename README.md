# firestore-export-gcs

Simple utility to export Firebase collection to Cloud Storage.

The outputs are Jsonlines compatible with this extension: https://github.com/firebase/extensions/tree/master/firestore-bigquery-export

### Why do you create this while there's existing one that comes with the extension?

Because the one that comes with the extension somehow doesn't work anymore for me and I have no time to debug the cause. It was working previously but wasn't anymore, I created this quickly so that I can continue doing my job :)

_If the official one works for you than you might don't need this._

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

Run it:

```bash
python3 export.py \
--project=my-project \
--source-collection=posts \
--dest-bucket=my-bucket
```

You will see something like this when it running:

```bash
[OK] 500 rows uploaded to gs://my-bucket/firestore_posts_export/doc1-to-doc500.json
[OK] 500 rows uploaded to gs://my-bucket/firestore_posts_export/doc501-to-doc1000.json
[OK] 500 rows uploaded to gs://my-bucket/firestore_posts_export/doc1001-to-doc1500.json
```

You can stop it using `CTRL+C` and start it again from the last document it exported using the `--start-after` argument.

```bash
python3 export.py \
--project=my-project \
--source-collection=posts \
--dest-bucket=my-bucket \
--start-after=doc1500
```

Optional arguments:

```bash
--batch-size 1000 (default=500)
--start-after abc-123 (document ID)
```
