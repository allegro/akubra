# Getting started

## Installation

### Build from source

You'll need go >= 1.10 compiler [see](https://golang.org/doc/install)

@@ TODO

## Configuration

In current version you'll need to configure all features at once. [Here] is example configuration,
keep reading to get a grip on sections details

### Service
This section defines how akubra will communicate with clients (`Server` subsection) and storage clusters (`Client` subsection).

#### Service.Server

Listen
: Address sting where server should bind to listen for connections

BodyMaxSize
: Maximum accepted body size. Recommended 100MB as S3 spec recommends to use multipart upload for larger objects. Type is HumanSizeUnits, and input could be expressed more human friendly fashion

MaxConcurrentRequests
: Max number of incoming requests to process at a time, when exceeded server would pause accepting connections

TechnicalEndpointListen:
: TechnicalEndpoint handles non business requests like configuration validation

HealthCheckEndpoint:
: Path where other services may find out about instance state

ReadTimeout
: ReadTimeout is client request max duration. Set to prevent problems from slow clients

WriteTimeout
: Write Timeout is server request max processing time

ShutdownTimeout
: Gracefull shoutdown duration limit

#### Service.Client

In this section we configure client side of akubra

DialTimeout
: Timeout for connection establishment

Transports
: More detailed configuration for storage clients see below

AdditionalRequestHeaders
: Additional headers added to original request. Handy for adding default `Cache-Control` if not set by client. If any header is set by client it is *not overriten* by `akubra`. Define the headers as `<name:value>` map. Example:

    AdditionalRequestHeaders:
      'Cache-Control': "public, s-maxage=600, max-age=600"

AdditionalResponseHeaders
: Simmilar option above, but those decorates responses from storage

##### Server.Client.Transports

For greater controll over client connection for different types of requests we provide Transport configurations. It's a list of `Transport` properties definitions, last element of that list is a default transport (in case no other rule matches). This feature is handy in examplle when we want to fail as fast as possible on long read but keep timeouts greater for writes

###### Transport entry

Name
: Name of the Transport entry

Rules
: Set of requirements to apply given transport properties. All have to be satissfied:

  Method
  : Regular expression to match HTTP method (DELETE, GET, HEAD, OPTIONS, POST, PUT)

  Path
  : Regular expression to match path

  QueryParam
  : Regular expression to match query params

Properties
: net/http `Transport` properties, applied when performing matched requests

    MaxIdleConns
    : see: https://golang.org/pkg/net/http/#Transport Zero means no limit.

    MaxIdleConnsPerHost
    : see https://golang.org/pkg/net/http/#Transport If zero, DefaultMaxIdleConnsPerHost is used.

    IdleConnTimeout
    : see: https://golang.org/pkg/net/http/#Transport. Zero means no limit.

    ResponseHeaderTimeout:
    : see: https://golang.org/pkg/net/http/#Transport

    DisableKeepAlives
    : see: https://golang.org/pkg/net/http/#Transport. Default false

In example below we shortened await time for read and expaded timeout for other operations

    Transports:
      -
        Name: Method:GET
        Rules:
          Method: GET
        Properties:
          MaxIdleConns: 0
          MaxIdleConnsPerHost: 100
          IdleConnTimeout: 0s
          ResponseHeaderTimeout: 2s
      -
        Name: DefaultTransport
        Rules:
        Properties:
          MaxIdleConns: 0
          MaxIdleConnsPerHost: 100
          IdleConnTimeout: 0s
          ResponseHeaderTimeout: 5s

### Storages

In this sections we name all storages used within system. Later on we'll compose them into replicas and shards.

Storage configuration fields

	Passthrough = "passthrough"
	// S3FixedKey will sign requests with single key
	S3FixedKey = "S3FixedKey"
	// S3AuthService will sign requests using key from external source
	S3AuthService = "S3AuthService"

Backend
: Storage entrypoint url
Type
: Type denotes backend specific authorization procedure.
  passthrough
  : no authorization manipulation, requests signed by akubra's client is passed unmodified.
  S3FixedKey
  : all even unsigned requests are signed by fixed pair of keys provided in `Properties` section
  S3AuthService
  : akubra will authorize and re-sign request based on internal credential store (see more in CredentialsStore)
Maintenance:
: boolean flag which puts storage into Maintenance mode. All writes to this storage will be queued to be syncronizes from replica (see more in Replication)
Properties
: Map of additional storage properties (like credentials for S3FixedTypes)





