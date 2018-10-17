const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const { initManagement } = require('../lib/management/index');
const { applyBucketIngestionWorkflows } =
    require('../extensions/ingestion/management');
const config = require('../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const mConfig = config.metrics;
const rConfig = config.redis;
const s3Config = config.s3;
const IngestionPopulator = require('../lib/queuePopulator/IngestionPopulator');
const zookeeper = require('node-zookeeper-client');

const { HealthProbeServer } = require('arsenal').network.probe;
const log = new werelogs.Logger('Backbeat:IngestionPopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

// TODO-FIX: Before proceeding, either in management, cloudserver, or here,
//           need to validate users input "host"/"port" source.
// NOTE: Validation is done in orbit when a new storage location is added

const activeIngestionSources = {};

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState, qConfig, log) {
    if (taskState.batchInProgress) {
        log.warn('skipping replication batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing replication batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    queuePopulator.processAllLogEntries({ maxRead }, (err, counters) => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during replication', {
                method: 'QueuePopulator::task.queueBatch',
                error: err,
            });
            return undefined;
        }
        const logFunc = (counters.some(counter =>
            Object.keys(counter.queuedEntries).length > 0) ?
            log.info : log.debug).bind(log);
        logFunc('replication batch finished', { counters });
        return undefined;
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

// TODO-FIX: update params to constructor
const ingestionPopulator = new IngestionPopulator(zkConfig, kafkaConfig,
    qpConfig, mConfig, rConfig, extConfigs, s3Config);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

// TODO: Thoughts on init
//   I think it would be easier to do the initial ingestion, listing and
//   putting into mongo in here. Then start up IngestionReader's once done with
//   initial task.
//   Would require recording some sort of offset/status of where we left off
//   in case user removes ingestion or pauses it.

function initAndStart() {
    initManagement({
        serviceName: 'ingestion',
        serviceAccount: extConfigs.ingestion.auth.account,
        applyBucketWorkflows: applyBucketIngestionWorkflows,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        async.series([
            done => ingestionPopulator.open(done),
            done => {
                const taskState = {
                    batchInProgress: false,
                };
                schedule.scheduleJob(qpConfig.cronRule, () => {
                    queueBatch(ingestionPopulator, taskState, qpConfig, log);
                });
                done();
            },
            done => {
                healthServer.onReadyCheck(log => {
                    const state = ingestionPopulator.zkStatus();
                    if (state.code === zookeeper.State.SYNC_CONNECTED.code) {
                        return true;
                    }
                    log.error(`Zookeeper is not connected! ${state}`);
                    return false;
                });
                log.info('Starting HealthProbe server');
                healthServer.start();
                done();
            },
        ], err => {
            if (err) {
                log.error('error during ingestion populator initialization', {
                    method: 'IngestionPopulator::task',
                    error: err,
                });
                process.exit(1);
            }

            // on init, setup active sources list
            const ingestionSources = config.getIngestionSourceList();
            extConfigs.ingestion.sources = ingestionSources;

            async.each(ingestionSources, (source, next) => {
                ingestionPopulator.addNewLogSource(source, err => {
                    if (err) {
                        log.error('error adding ingestion source', {
                            source: source.name,
                            prefix: source.prefix,
                            type: source.type,
                            method: 'IngestionPopulator::task',
                        });
                        return next(err);
                    }
                    activeIngestionSources[source.name] = true;
                    return next();
                });
            }, err => {
                if (err) {
                    // fatal on startup
                    process.exit(1);
                }
            });
        });
    });
}


// dynamically add/remove sources
config.on('ingestion-source-list-update', () => {
    // TODO-FIX: should each source be name + prefix?
    // We can potentially have buckets with same name in different locations
    // addNewLogSource(newSource, cb)
    // closeLogState(sourceName, done)
    const ingestionSources = config.getIngestionSourceList();
    extConfigs.ingestion.sources = ingestionSources;

    const activeSources = Object.keys(activeIngestionSources);
    const updatedSources = ingestionSources.map(i => i.name);
    const allSources = [...new Set(activeSources
        .concat(updatedSources))];

    async.each(allSources, (source, next) => {
        if (updatedSources.includes(source)) {
            if (!activeSources.includes(source)) {
                ingestionPopulator.addNewLogSource(source, err => {
                    if (err) {
                        log.error('error adding ingestion source', {
                            source: source.name,
                            prefix: source.prefix,
                            type: source.type,
                            method: 'IngestionPopulator::task',
                        });
                        return next(err);
                    }
                    activeIngestionSources[source.name] = true;
                    return next();
                });
            }
        } else {
            // this source is no longer in configs
            ingestionPopulator.closeLogState(source.name, err => {
                if (err) {
                    log.error('error removing ingestion source', {
                        source: source.name,
                        prefix: source.prefix,
                        type: source.type,
                        method: 'IngestionPopulator::task',
                    });
                    return next(err);
                }
                delete activeIngestionSources[source.name];
                return next();
            });
        }
    }, err => {
        if (err) {
            process.exit(1);
        }
    });
});

initAndStart();

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    ingestionPopulator.close(() => {
        process.exit(0);
    });
});
