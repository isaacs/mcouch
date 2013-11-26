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
    -q --seq=SEQ                        Start at SEQ
    -Q --seq-file=FILE                  Store sequence number in FILE
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
