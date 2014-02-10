# manta-couch

Put your CouchDB in Manta, attachments and docs and all

## Usage

In JavaScript

```javascript
var mantaCouch = require('manta-couch')

var mc = mantaCouch({
  client: myMantaClient,
  path: '~~/stor/my-couchdb',
  db: 'http://localhost:5984/my-couchdb',

  // If specified, will save sequence in this file, and read from it
  // on startup.  Highly recommended to avoid extra PUTs!
  seqFile: '.sequence',

  // you can also optionally specify a numeric sequence here:
  seq: 1234
})
```

In the command line:

```bash
mcouch http://localhost:5984/my-couch ~~/stor/my-couchdb
```

Full options:

```
mcouch - Relax with the Fishes
Usage: mcouch [args] COUCHDB MANTAPATH

    COUCHDB                             Full url to your couch, like
                                        http://localhost:5984/database
    MANTAPATH                           Remote path in Manta, like
                                        ~~/stor/database
    -Q FILE, --seq-file=FILE            File to store the sequence in
    -q NUMBER, --seq=NUMBER             Sequence ID to start at
    --inactivity-ms=MS                  Max ms to wait before assuming
                                        disconnection.
    -d, --delete                        Delete removed attachments and docs from
                                        manta
    -f, --forensic                      Track changes in a non-destructive way
    -a ACCOUNT, --account=ACCOUNT       Manta Account (login name)
    -h, --help                          Print this help and exit
    -i, --insecure                      Do not validate SSL certificate
    -k FINGERPRINT, --keyId=FINGERPRINT SSH key fingerprint
    -u URL, --url=URL                   Manta URL
    -v, --verbose                       verbose mode
```

When using the command line, you can also specify all the typical
MANTA environment variables, or you can pass the typical Manta
command-line arguments.

## Manta Folder Structure

In non-forensic mode, mcouch will put the doc contents at
`{path}/{id}/doc.json`, and store attachments at
`{path}/{id}/_attachments/{filename}`.

If you enable deletes, then it will also permanently delete documents
and attachments when they are removed.  If delete is not enabled, then
removed attachments will remain in the `_attachments` folder, and
removed docs will not be removed.

In forensic mode, in addition to storing a `doc.json`, this is linked
to `{path}/{id}/{rev}.json` where `{rev}` is the document revision id.
Attachments are linked to
`{path}/{id}/_attachments/{filename}-{digest}`.  This is so that, even
as the documents or attachments are modified, past versions can still
be reconstructed.  Deleting an attachment will remove its main file,
but the link to the digest-tagged version will remain.

In forensic mode, deletes do not remove data permanently from Manta.
Instead, deleting a document will cause the doc and all its
attachments to be moved to a `_deleted-{index}` folder, and a
`_deleted:true` record will be written to `doc.json`.  The `{index}`
value on the trash folder is incremented with each subsequent delete.
