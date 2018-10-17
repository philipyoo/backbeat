const async = require('async');
const AWS = require('aws-sdk');
const werelogs = require('werelogs');

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

// Can we use service account to perform all aws actions for us?
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

// TODO:
// Maybe instead of creating a new storage location, we can just setup a new
// ingestion and the user will need to specify the source storage info on
// creation of workflow. This will avoid overcrowding the list of storage
// locations.

// For pause/resume consideration, have a field/flag "enabled"
// -> "enabled": true/false

/*
workflow = {
    sourceBucketName: '', // bucket to ingest from
    zenkoBucketName: '',  // bucket to ingest to
    locationName: '',     // source location name - user defined location name
    locationType: '',     // i.e. 'location-azure-v1'
    host: '',
    port: 8000,
    transport: '',
    enabled: true, // for later use (maybe pause/resume)
}

sources: [
    {
        "sourceBucketName": "",       // source bucket name (i.e. ring, aws)
        "zenkoBucketName": "",
        "host": "",
        "port": 8000,
        "transport": "",
        "locationName": "",     // my-azure-site
        "locationType": "",     // 'location-azure-v1'

        // can't tell if below are needed
        "locationName": "",     // source location name
    }
]
*/

function _addToIngestionSourceList(bucketName, workflows, cb) {
    // A bucket source can only have a single workflow
    if (workflows.length > 1) {
        logger.fatal('workflow count is greater than 1', {
            method: 'management::ingestion::_addToIngestionSourceList',
        });
        const error = {
            message: 'something went wrong in ingestion workflow updates',
            name: 'management::ingestion',
            code: 500,
        };
        return cb(error);
    }

    const wf = workflows[0];

    if (wf.sourceBucketName !== bucketName) {
        logger.fatal('management overlay data mismatch');
        const error = {
            message: 'something went wrong in ingestion workflow updates',
            name: 'management::ingestion',
            code: 500,
        };
        return cb(error);
    }

    const newIngestionSource = {
        // bucket to ingest from
        sourceBucketName: wf.sourceBucketName,
        // bucket to ingest to
        zenkoBucketName: wf.zenkoBucketName,
        locationType: locationTypeMatch[wf.locationType],
        host: wf.host,
        port: wf.port,
        transport: wf.transport,
        // enabled: wf.enabled,          // support for pause/resume
    };

    const currentList = config.getIngestionSourceList();
    const indexPos = currentList.findIndex(i =>
        i.sourceBucketName === newIngestionSource.sourceBucketName);

    // if updating an existing ingestion source
    if (indexPos >= 0) {
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
            // TODO-FIX:
            // zenko bucket should be brand new. If any error occurs here,
            // that means the bucket name is taken, so error out.
            // To best avoid management errors, a pre-check should be done
            // prior to reaching management stage here.
            // Maybe orbit can check to see if bucket exists on ingestion
            // setup. If it doesn't exist, then allow setup.
            const params = {
                Bucket: wf.zenkoBucketName,
                // TODO: do we specify location name as location constraint
                CreateBucketConfiguration: {
                    LocationConstraint: wf.locationName,
                },
            };
            s3client.createBucket(params, next);
        },
        // default it to versioned bucket
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
        logger.fatal('something went wrong in workflow updates', {
            method: 'management::ingestion::_removeFromIngestionSourceList',
        });
        const error = {
            message: 'something went wrong in ingestion workflow updates',
            name: 'management::ingestion',
            code: 500,
        };
        return cb(error);
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
