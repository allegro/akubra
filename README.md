# Akubra [![Build Status](https://travis-ci.org/allegro/akubra.svg?branch=master)](https://travis-ci.org/allegro/akubra) #
## Project goal ##

Akubra is a simple solution to keep independent S3 storages in sync eventually (almost realtime).

Keeping redundant storage clusters which handle great volume of new objects (~300k/h), is most
effective by feeding them with all data incoming at once. That's what Akubra does, with minimum
memory and cpu footprint.

Syncronizing S3 storages offline is almost impossible with high volume traffic. It would require
keeping track of new objects (or periodical buckets listing...), downloading and uploading them to
other storage. It's slow, expensive and hard to implement.

Akubra way is to put files in all storages at once by copying requests to multiple backends. Sometimes
one of clusters may reject request for various reason, but that's not a big deal: we simply log
that event, and sync object in the independent process.

## Build

### Prerequisites

You need go >= 1.7 compiler [see](https://golang.org/doc/install)

### Build
In main directory of this repository do:

    make build

### Test

    make test

## Usage of akubra:

    usage: akubra [<flags>]

    Flags:
          --help       Show context-sensitive help (also try --help-long and --help-man).
      -c, --conf=CONF  Configuration file e.g.: "conf/dev.json"



### Example:

    akubra -c devel.json

## How it works?

Once request comes to our proxy we copy all it's headers and create pipes for
body streaming to each endpoint. If any endpoint returns positive response it's
immediately returned to a client. If all endpoints return error first response
passed to a client.

If some nodes respond incorrectly we log which cluster has a problem, is it
storing or reading and where the erroneous file may be found. In that case
we also return positive response as stated above.

We also handle slow endpoint scenario. If there are more connections than safe
limit defined in configuration, the backend with most of them is taken out of
the pool and error is logged.


## Configuration ##

Configuration is read from the yaml configuration file with the following fields:

    # Listen interface and port e.g. "0:8000", "localhost:9090", ":80"
    Listen: ":8080"
    # List of backend URI's e.g. "http://s3.mydaracenter.org"
    Backends:
      - "http://s3.dc1.internal"
      - "http://s3.dc2.internal"
    # Limit of outgoing connections. When limit is reached, akubra will omit external backend
    # with greatest number of stalled connections
    ConnLimit: 100
    # Additional not amazon specific headers proxy will add to original request
    AdditionalResponseHeaders:
        'Access-Control-Allow-Origin': "*"
        'Access-Control-Allow-Credentials': "true"
        'Access-Control-Allow-Methods': "GET, POST, OPTIONS"
        'Access-Control-Allow-Headers': "DNT,X-CustomHeader,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type"
    # Additional headers added to backend response
    AdditionalRequestHeaders:
        'Cache-Control': "public, s-maxage=600, max-age=600"
        'X-akubra-version': '0.9.26'
    # Read timeout on outgoing connections
    ConnectionTimeout: "3s"
    # Dial timeout on outgoing connections
    ConnectionDialTimeout: "1s"
    # Backend in maintenance mode. Akubra will skip this enpoint

    # MaintainedBackend: "http://s3.dc2.internal"

    # List request methods to be logged in synclog in case of backend failure
    SyncLogMethods:
      - PUT
      - DELETE


## Limitations

 * Users credentials have to be identical on every backend
 * We do not support S3 partial uploads
