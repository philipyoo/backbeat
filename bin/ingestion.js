const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;

const IngestionPopulator = require('../lib/queuePopulator/IngestionPopulator');
const config = require('../conf/Config');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const ingestionExtConfigs = config.extensions.ingestion;
const qpConfig = config.queuePopulator;
const mConfig = config.metrics;
const rConfig = config.redis;
const s3Config = config.s3;

const log = new werelogs.Logger('Backbeat:IngestionPopulator');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

// TODO: based on source list config changes, dynamically add/remove
const activeIngestionSources = [];

/* eslint-disable no-param-reassign */
function queueBatch(ingestionPopulator, taskState, qConfig, log) {
    if (taskState.batchInProgress) {
        log.warn('skipping ingestion batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing ingestion batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    ingestionPopulator.processLogEntries({ maxRead }, err => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during ingestion', {
                method: 'IngestionPopulator::task.queueBatch',
                error: err,
            });
        }
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

function initIngestionReaders(ingestionPopulator, cb) {
    // TODO: use config level updated sources list. for now stick with static
    const ingestionSources = ingestionExtConfigs.sources;

    return async.each(ingestionSources, (source, next) => {
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
            // TODO: config change. Should somehow depend on:
            //       Storage Location Name + Local Zenko Bucket Name
            const key = `${source.prefix}:${source.name}`;
            activeIngestionSources.push(key);
            return next();
        });
    }, cb);
}

const ingestionPopulator = new IngestionPopulator(zkConfig, kafkaConfig,
    qpConfig, mConfig, rConfig, ingestionExtConfigs, s3Config);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

async.waterfall([
    done => ingestionPopulator.open(done),
    // on init, setup active sources ingestion readers
    done => initIngestionReaders(ingestionPopulator, done),
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

    // TODO: dynamic add/remove checks here
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    ingestionPopulator.close(() => {
        process.exit(0);
    });
});
