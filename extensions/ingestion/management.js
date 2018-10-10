const werelogs = require('werelogs');

const config = require('../../conf/Config');

const logger = new werelogs.Logger('mdManagement:ingestion');

/*
Structure of a workflow
{
    "ingestion": {
        "my-source-bucket": [
            {
                "bucketName": '',
                "enabled": true/false,    // can use this for pause/resume
                                          // managed in this file
                "name": '', // UI purposes
                "workflowId": '',
                "host": '',
                "port": 1111,
                "transport": 'http',
                "type": '', // is this needed?
            }
        ]
    }
}
*/

/*
// suggestion for updating source list configs
sources: [
    {
        "name": "my-bucket-name",
        "prefix": "",  // is this needed?
        "logSource": "bucketd",  // options === ['bucketd','mongo','dmd']
        "host": "127.0.0.1",  // will this look the same for external srcs like aws
        "port": "9000",
        "transport": "http",
        // some sort of credentials
    }
]
*/

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

    // TODO: refine this config
    // Is prefix needed? Move cronRule out?
    const newIngestionSource = {
        name: bucketName,  // want to change this to key: "bucketName"
        prefix: bucketName,  // want to consider prefix to be source name
        cronRule: '*/5 * * * * *',
        zookeeperSuffix: `/${bucketName}`, // can remove this
        host: wf.host,
        port: wf.port,
        https: wf.transport === 'https',
        type: 'scality',  // TODO-FIX: type?
        raftCount: 8,  // remove this?
        // need credentials
        // enabled: wf.enabled,          // support for pause/resume
    };
    // TODO: should workflows have joi validation

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
