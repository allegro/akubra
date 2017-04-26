# Akubra
[![Version Widget]][Version] [![Build Status Widget]][Build Status] [![GoDoc Widget]][GoDoc]

[Version]: https://github.com/allegro/akubra/releases/latest
[Version Widget]: https://img.shields.io/github/release/allegro/akubra.svg
[Build Status]: https://travis-ci.org/allegro/akubra
[Build Status Widget]: https://travis-ci.org/allegro/akubra.svg?branch=master
[GoDoc]: https://godoc.org/github.com/allegro/akubra
[GoDoc Widget]: https://godoc.org/github.com/allegro/akubra?status.svg

## Goals

### Redundancy

Akubra is a simple solution to keep an independent S3 storages in sync - almost
realtime, eventually consistent.

Keeping synchronized storage clusters, which handles great volume of new objects
(about 300k obj/h), is the most efficient by feeding them with all incoming data
at once. That's what Akubra does, with a minimum memory and cpu footprint.

Synchronizing S3 storages offline is almost impossible with a high volume traffic.
It would require keeping track of new objects (or periodical bucket listing),
downloading and uploading them to other storage. It's slow, expensive and hard
to implement robustly.

Akubra way is to put files in all storages at once by copying requests to multiple
backends. I case one of clusters rejects request it logs that event, and syncronizes
troublesome object with an independent process.

### Seamless storage space extension with new storage clusters
Akubra has sharding capabilities. You may easily configure new backends with
weigths and append them to clients clusters pool.

Based on clusters weights akubra splits all operations between clusters in pool.
It also backtracks to older cluster when requested for not existing object on
target cluster. This kind of events are logged, so it's possible to rebalance
clusters in background.

## Build

### Prerequisites

You need go >= 1.7 compiler [see](https://golang.org/doc/install)

### Build
In main directory of this repository do:

```
make build
```

### Test

```
make test
```

## Usage of Akubra:

```
usage: akubra [<flags>]

Flags:
      --help       Show context-sensitive help (also try --help-long and --help-man).
  -c, --conf=CONF  Configuration file e.g.: "conf/dev.yaml"
```

### Example:

```
akubra -c devel.yaml
```

## How it works?

Once a request comes to our proxy we copy all its headers and create pipes for
body streaming to each endpoint. If any endpoint returns a positive response it's
immediately returned to a client. If all endpoints return an error, then the
first response is passed to the client

If some nodes respond incorrectly we log which cluster has a problem, is it
storing or reading and where the erroneous file may be found. In that case
we also return positive response as stated above.

We also handle slow endpoint scenario. If there are more connections than safe
limit defined in configuration, the backend with most of them is taken out of
the pool and error is logged.


## Configuration ##

Configuration is read from a YAML configuration file with the following fields:

```yaml
# Listen interface and port e.g. "127.0.0.1:9090", ":80"
Listen: ":8080"
# Technical endpoint interface
TechnicalEndpointListen: ":8071"
# Additional not AWS S3 specific headers proxy will add to original request
AdditionalRequestHeaders:
    'Cache-Control': "public, s-maxage=600, max-age=600"
    'X-Akubra-Version': '0.9.26'
# Additional headers added to backend response
AdditionalResponseHeaders:
    'Access-Control-Allow-Origin': "*"
    'Access-Control-Allow-Credentials': "true"
    'Access-Control-Allow-Methods': "GET, POST, OPTIONS"
    'Access-Control-Allow-Headers': "DNT,X-CustomHeader,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type"
# MaxIdleConns see: https://golang.org/pkg/net/http/#Transport
# Default 0 (no limit)
MaxIdleConns: 0
# MaxIdleConnsPerHost see: https://golang.org/pkg/net/http/#Transport
# Default 100
MaxIdleConnsPerHost: 100
# IdleConnTimeout see: https://golang.org/pkg/net/http/#Transport
# Default 0 (no limit)
IdleConnTimeout: 0s
# ResponseHeaderTimeout see: https://golang.org/pkg/net/http/#Transport
# Default 5s
ResponseHeaderTimeout: 5s
# DisableKeepAlives see: https://golang.org/pkg/net/http/#Transport
# Default false

DisableKeepAlives: false

# Maximum accepted body size
BodyMaxSize: "100M"
# Backend in maintenance mode. Akubra will skip this endpoint

# MaintainedBackends:
#  - "http://s3.dc2.internal"

# List request methods to be logged in synclog in case of backend failure
SyncLogMethods:
  - PUT
  - DELETE
# Configure sharding
Clusters:
  # Cluster label
  cluster1:
    # Set type, available: replicator
    Type: "replicator"
    # Weight
    Weight: 1
    Backends:
      - http://s3.dc1.internal
  cluster2:
    Type: "replicator"
    Weight: 1
    Backends:
      - http://s3.dc2.internal
Client:
  Name: client1
  Clusters:
    - cluster1
    - cluster2
  ShardsCount: 10000

Logging:
  Synclog:
    stderr: true
  #  stdout: false  # default: false
  #  file: "/var/log/akubra/sync.log"  # default: ""
  #  syslog: LOG_LOCAL1  # default: LOG_LOCAL1
  #  database:
  #    user: dbUser
  #    password: ""
  #    dbname: dbName
  #    host: localhost
  #    inserttmpl: |
  #      INSERT INTO tablename(path, successhost, failedhost, ts,
  #       method, useragent, error)
  #      VALUES ('new','{{.path}}','{{.successhost}}','{{.failedhost}}',
  #      '{{.ts}}'::timestamp, '{{.method}}','{{.useragent}}','{{.error}}');

  Mainlog:
    stderr: true
  #  stdout: false  # default: false
  #  file: "/var/log/akubra/akubra.log"  # default: ""
  #  syslog: LOG_LOCAL2  # default: LOG_LOCAL2
  #  level: Error   # default: Debug

  Accesslog:
    stderr: true  # default: false
  #  stdout: false  # default: false
  #  file: "/var/log/akubra/access.log"  # default: ""
  #  syslog: LOG_LOCAL3  # default: LOG_LOCAL3

# Enable metrics collection
Metrics:
  # Possible targets: "graphite", "expvar", "stdout"
  Target: graphite
  # Expvar handler listener address
  ExpAddr: ":8080"
  # How often metrics should be released, applicable for "graphite" and "stdout"
  Interval: 30s
  # Graphite metrics prefix path
  Prefix: my.metrics
  # Shall prefix be suffixed with "<hostname>.<process>"
  AppendDefaults: true
  # Graphite collector address
  Addr: graphite.addr.internal:2003
  # Debug includes runtime.MemStats metrics
  Debug: false
```

## Configuration validation for CI

Akubra has technical http endpoint for configuration validation puroposes.
It's configured with TechnicalEndpointListen property.

### Example usage

    curl -vv -X POST -H "Content-Type: application/yaml" --data-binary @akubra.cfg.yaml http://127.0.0.1:8071/validate/configuration

Possible resposes:

    * HTTP 200
    Configuration checked - OK.
or:

    * HTTP 400, 405, 413, 415 and info in body with validation error message


## Limitations

 * User's credentials have to be identical on every backend
 * We do not support S3 partial uploads
