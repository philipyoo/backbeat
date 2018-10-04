# Metadata Ingestion from RING to Zenko

## Description

This is the design document discussing the ingestion process of existing metadata
from the RING into Zenko with MongoDB.

The primary use case is to copy the existing metadata from the RING/S3 Connector
and ingest into the MongoDB backend that is used with Zenko so that users can
connect existing buckets on RING to be managed by Zenko. This would allow users
to complete operations such as metadata search and CRR. The ingestion process will
also enable out-of-band updates. This will allow operations made directly to the RING
to update and be reflected on Zenko. 

This specific development will allow Zenko instances to copy and ingest existing
metadata, copying the information from the raft logs completely and syncing MongoDB
to be a parallel metadata database.

## Purpose

The primary purpose is to copy all existing metadata from pre-existing storage solutions
to MongoDB in Zenko. The imported metadata will allow users to manage the bucket
and search the metadata that is stored in MongoDB.

## Design

The ingestion process would be initiated bucket by bucket, as specified by the user.

The proposed work flow will be as follows:
* A RING bucket will be added as a storage location. An ingestion workflow will be
  applied to a bucket created by the user using the created storage location, and
  the option for `bucketMatch` will be true. This will allow 1 to 1 mapping between
  the bucket defined through Zenko and the RING bucket.
* Upon initiating the ingestion workflow:
    * Record the last sequence ID from the Metadata Server (send a query using the
      `metadataWrapper`) that is part of the S3 connector - this will serve as a
      marker that will be stored as a `log offset` to bookmark the point between
      logs that existed prior to the start of the Zenko instance, and the point
      where new logs are added during the process where the old logs are copied.
    * From backbeat, we will use the S3 protocols to send the following requests
      to the S3 connector: 
        * For the specified bucket, list all objects in the buckets. This will return
          info such as the `object key`, and if `versioning` is enabled, `version id`.
      Using the list of object keys, send a query directly to the metadata server
      with the `metadataWrapper` class in Arsenal.
        * This will get the JSON object for each object, which is put into kafka.

With this design, Backbeat will include a new populator which reads from the Metadata
server and queues the logs to Kafka. The consumer will not need to be changed.

On deployment of Zenko, we will have 1 ingestion populator pod and 5 ingestion processors
pods. Each populator pod can host multiple ingestion populators, while each each processors
pod can host multiple consumers.
Each instance of an ingestion populator should control one and only one ingestion
workflow. The logReader for each ingestion populator will be setup to source logs
for the specified ingestion bucket.

## Dependencies

* MongoDB
* Existing S3 Connector
* Backbeat (including Backbeat dependencies, e.g. Zookeeper and Kafka)

### Bucket/Object-Key Parity

The ingestion source is a bucket on RING registered as a storage location. When
selecting the ingestion source and creating the destination bucket, `bucketMatch`
will be set to true. This will limit the ingestion source to only allow 1 bucket
attached to it, but this can ensure an exact match of object names (unlike the
prefix schema used in Zenko locations that host multiple buckets).

## Background Information for Design

* The `sequence ID` from Metadata Server will be in numerical order.
* There will not be data that is put directly into the Zenko cloudserver - all
  data will be sent to the S3 Connector, and logs will come from Metadata Server.

## Rejected Options

* The option that was originally considered was to ingest entire RINGs in one setting,
  upon starting Zenko. The ingestion populator and consumers would be created automatically
  and begin listing all buckets and objects on the RING.
    * For large RINGs with many objects, this would be a very heavy workload that
      could take extremely long periods of time to run to completion.
    * This also limits the flexibility for users, if they only wanted to ingest
      and use one bucket with Zenko, they would still have to wait until all buckets
      and objects were processed.
    * This implementation also required a fresh install of Zenko, which would bar
      users from making any changes or updates to the ingestion process after the
      initial deployment.

## Diagram of work flow, to be updated

```
                    +-------------+
                    |             |
                    | Cloudserver |
                    |             |
                    +------+------+   Use S3 calls to
                           |          list buckets and  +----------------------+
                           |          objects that are  |                      |
                           |          in the pre-       |     S3 Connector     |
                           |          existing backend  |                      |
                           |        +-----------------> | +------------------+ |
                           |        |                   | |                  | |
                           v        | ----------------> | |  Metadata Server | |
                  +-----------------+  Obtain list of   | |                  | |
                  | +-------------+ |  object keys      | +--------^---------+ |
                  | |             | |                   +----------|-----------+
+----------+      | |  Kafka      | |                              |
|          | <------+             | |                              |
| Mongo DB |      | +-------------+ |                              |
|          |      | |             | +------------------------------+
+----------+   +-----+ Zookeeper  | |  Using the list of object keys,
               |  | |             | |  call the Metadata Server directly
               |  | +-------------+ |  to get JSON of each object
               |  |                 |
               |  |    Backbeat     |
               |  |                 |
               |  +-----------------+
               |
               |     On startup, check Zookeeper to
               +---> see if a 'sequence id' already
                        exists.

```
