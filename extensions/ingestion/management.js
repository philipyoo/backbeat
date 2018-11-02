const async = require('async');
const AWS = require('aws-sdk');

const werelogs = require('werelogs');
const { errors } = require('arsenal');

const config = require('../../conf/Config');
const management = require('../../lib/management/index');

const logger = new werelogs.Logger('mdManagement:ingestion');

const locationTypeMatch = {
    'location-mem-v1': 'mem',
    'location-file-v1': 'file',
    'location-azure-v1': 'azure',
    'location-do-spaces-v1': 'aws_s3',
    'location-aws-s3-v1': 'aws_s3',
    'location-wasabi-v1': 'aws_s3',
    'location-gcp-v1': 'gcp',
    'location-scality-ring-s3-v1': 'aws_s3',
    'location-ceph-radosgw-s3-v1': 'aws_s3',
};

function getS3Client() {
    const cfg = config.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;
    const serviceCredentials =
        management.getLatestServiceAccountCredentials();
    // FIXME
    const keys = serviceCredentials.accounts[0].keys;
    const credentials = new AWS.Credentials(keys.access, keys.secret);
    const s3Client = new AWS.S3({
        endpoint,
        sslEnabled: false,
        credentials,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        httpOptions: { timeout: 0 },
        maxRetries: 3,
    });
    return s3Client;
}

/*
// Data I need from workflow:

workflow = {
    locationName: '', // source location name saved in storage locations
    locationType: '', // i.e. 'location-azure-v1'
    sourceBucketName: '', // source bucket name (in our case from RING)
    zenkoBucketName: '', // local bucket name specified by user on
                         //ingestion setup
    host: "", // how to get this?
    port: 8000, // how to get this?
    transport: "",  // how to get this?
    enabled: true/false, // for pause/resume, toggled in ingesiton workflow page
}

// Example source list item:

sources: [
    {
        "locationName": "",
        "locationType": "",
        "sourceBucketName": "",
        "zenkoBucketName": "",
        "host": "",
        "port": 8000,
        "transport": "",
        "enabled": true,
    }
]
*/

function _addToIngestionSourceList(bucketName, workflows, cb) {
    const wf = workflows[0];

    // a bucket source can only have a single workflow
    if (workflows.length > 1 || wf.sourceBucketName !== bucketName) {
        logger.fatal('invalid ingestion workflow', {
            method: 'management::ingestion::_addToIngestionSourceList',
            bucketMismatch: wf.sourceBucketName !== bucketName,
            incorrectForm: workflows.length > 1,
        });
        return cb(errors.InternalError.customizeDescription(
            'invalid workflow update'));
    }

    const newIngestionSource = {
        // bucket to ingest from
        sourceBucketName: wf.sourceBucketName,
        // bucket to ingest to
        zenkoBucketName: wf.zenkoBucketName,
        locationType: locationTypeMatch(wf.locationType),
        host: wf.host,
        port: wf.port,
        transport: wf.transport,
        enabled: wf.enabled, // support for pause/resume
    };

    const currentList = config.getIngestionSourceList();
    const indexPos = currentList.findIndex(i =>
        i.sourceBucketName === newIngestionSource.sourceBucketName);

    // if updating an existing ingestion source
    if (indexPos !== -1) {
        logger.debug('updated new ingestion source', {
            bucket: bucketName,
            method: 'management::ingestion::_addToIngestionSourceList',
        });
        // this is an update to the ingestion source
        currentList.splice(indexPos, 1, newIngestionSource);
        config.setIngestionSourceList(currentList);
        return cb();
    }

    // if adding a new ingestion source
    logger.debug('added new ingestion source', {
        bucket: bucketName,
        method: 'management::ingestion::_addToIngestionSourceList',
    });

    currentList.push(newIngestionSource);
    config.setIngestionSourceList(currentList);

    const s3client = getS3Client();
    return async.series([
        next => {
            // TODO: a pre-check HEAD on bucket name should be made in orbit.
            // I think these pre-checks are made for other services already
            const params = {
                Bucket: wf.zenkoBucketName,
                CreateBucketConfiguration: {
                    LocationConstraint: 'us-east-1',
                },
            };
            s3client.createBucket(params, next);
        },
        // TODO: default to versioned bucket?
        next => {
            const params = {
                Bucket: wf.zenkoBucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            };
            s3client.putBucketVersioning(params, next);
        },
    ], cb);
}

function _removeFromIngestionSourceList(bucketName, cb) {
    const currentList = config.getIngestionSourceList();

    if (!currentList[bucketName]) {
        logger.fatal('invalid ingestion workflow, source bucket name missing', {
            method: 'management::ingestion::_removeFromIngestionSourceList',
            bucket: bucketName,
        });
        return cb(errors.InternalError.customizeDescription(
            'invalid workflow update'));
    }

    delete currentList[bucketName];
    config.setIngestionSourceList(currentList);

    logger.debug('removed source from ingestion source list', {
        bucket: bucketName,
        method: 'management::ingestion::_removeFromIngestionSourceList',
    });
    return cb();
}

function applyBucketIngestionWorkflows(bucketName, bucketWorkflows,
                                       workflowUpdates, cb) {
    if (bucketWorkflows.length > 0) {
        _addToIngestionSourceList(bucketName, bucketWorkflows, cb);
    } else {
        _removeFromIngestionSourceList(bucketName, cb);
    }
}

module.exports = {
    applyBucketIngestionWorkflows,
};
