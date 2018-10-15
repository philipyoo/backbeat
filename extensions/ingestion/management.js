const AWS = require('aws-sdk');
const url = require('url');
const werelogs = require('werelogs');

const config = require('../../conf/Config');
const management = require('../../lib/management/index');

const logger = new werelogs.Logger('mdManagement:ingestion');

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

// suggestion for updating source list configs
// How do we determine logSource for locations besides ring?
// -> "logSource": "bucketd",  // options === ['bucketd','mongo','dmd']
// For pause/resume consideration, have a field/flag "enabled"
// -> "enabled": true/false

/*
wf = {
    bucketName: '',
    accessKey: '',
    secretKey: '',
    endpoint: '',  // optional (i.e. ring)
    enabled: true, // for later use (maybe pause/resume)
}


sources: [
    {
        "name": "",             // ingestion workflow name
        "accessKey": "",        // source creds
        "secretKey": "",
        "bucketName": "",       // source bucket name
        "locationName": "",     // "us-east-1"
        "locationType": "",     // "aws_s3", "gcp", "azure", "scality"
        // optional - ring requires
        "details": {
            "host": "",
            "port": "",
            "transport": "",
        }
    }
]
*/

// TODO: Determine logSource in management
//   i.e. "bucketd", "mongo", "dmd", etc

function _addToIngestionSourceList(bucketName, workflows, cb) {
    // A bucket source can only have a single workflow
    if (workflows.length > 1) {
        logger.fatal('something went wrong in workflow updates', {
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

    const details = {};
    if (wf.endpoint) {
        const { protocol, hostname, port } = url.parse(wf.endpoint);

        details.host = hostname;
        details.port = port;
        details.transport = protocol.slice(0, -1);
    }

    const newIngestionSource = {
        bucketName: wf.bucketName,
        accessKey: wf.accessKey,
        secretKey: wf.secretKey,
        details,
        // how to get the following?
        locationType: 'scality',  // 'aws_s3', 'gcp', 'azure', 'scality'

        type: 'scality',  // TODO-FIX: type?
        // enabled: wf.enabled,          // support for pause/resume
    };

    const currentList = config.getIngestionSourceList();
    currentList[bucketName] = newIngestionSource;
    config.setIngestionSourceList(currentList);

    logger.debug('added or updated an ingestion source', {
        bucket: bucketName,
        method: 'management::ingestion::_addToIngestionSourceList',
    });
    return cb();
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
