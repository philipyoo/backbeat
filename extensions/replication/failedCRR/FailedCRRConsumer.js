'use strict'; // eslint-disable-line strict
const async = require('async');

const Logger = require('werelogs').Logger;
const redisClient = require('../../replication/utils/getRedisClient')();

const FailedCRRProducer = require('./FailedCRRProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const config = require('../../../conf/Config');

// BackbeatConsumer constant defaults
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const CONCURRENCY = 10;

class FailedCRRConsumer {
    /**
     * Create the retry consumer.
     */
    constructor() {
        this._repConfig = config.extensions.replication;
        this._kafkaConfig = config.kafka;
        this._topic = config.extensions.replication.replicationFailedTopic;
        this.logger = new Logger('Backbeat:FailedCRRConsumer');
        this._failedCRRProducer = new FailedCRRProducer(this.kafkaConfig);
        this._backbeatTask = new BackbeatTask();
    }

    /**
     * Start the retry consumer by subscribing to the retry kafka topic. Setup
     * the failed CRR producer for pushing any failed redis operations back to
     * the queue.
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    start(cb) {
        let consumerReady = false;
        const consumer = new BackbeatConsumer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._topic,
            groupId: 'backbeat-retry-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', err => {
            if (!consumerReady) {
                this.logger.fatal('could not setup a backbeat consumer', {
                    method: 'FailedCRRConsumer.start',
                    error: err,
                });
                process.exit(1);
            }
        });
        consumer.on('ready', () => {
            consumerReady = true;
            consumer.subscribe();
            this.logger.info('retry consumer is ready to consume entries');
        });
        return this._failedCRRProducer.setupProducer(err => {
            if (err) {
                this.logger.error('could not setup producer', {
                    method: 'FailedCRRConsumer.processKafkaEntry',
                    error: err,
                });
                return cb(err);
            }
            return cb();
        });
    }

    /**
     * Process an entry from the retry topic, and set the data in a Redis hash.
     * @param {Object} kafkaEntry - The entry from the retry topic
     * @param {function} cb - The callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, cb) {
        const log = this.logger.newRequestLogger();
        let data;
        try {
            data = JSON.parse(kafkaEntry.value);
        } catch (err) {
            log.error('error processing retry entry', {
                method: 'FailedCRRConsumer.processKafkaEntry',
                error: err,
            });
            log.end();
            return cb();
        }
        return async.series([
            next => this._removePreexistingKeys(data, err => {
                if (err) {
                    this.logger.error('error deleting preexisting CRR object ' +
                    'metrics redis keys', {
                        method: 'FailedCRRConsumer.processKafkaEntry',
                        error: err,
                    });
                }
                return next();
            }),
            next => this._setRedisKey(data, kafkaEntry, log, next),
        ], cb);
    }

    /**
     * Delete any pre-existing object CRR metrics keys. This way the object
     * metrics during the retry will not be affected by the prior attempt.
     * @param {Object} data - The kafka entry value
     * @param {Function} cb - callback to call
     * @return {undefined}
     */
    _removePreexistingKeys(data, cb) {
        // eslint-disable-next-line no-unused-vars
        const [service, extension, status, bucketName, objectKey, versionId,
            site] = data.key.split(':');
        // CRR object-level metrics key schema.
        let queryString = `${site}:${bucketName}:${objectKey}:${versionId}:` +
            `${service}:${extension}:*`;
        return redisClient.scan(queryString, undefined, (err, keys) => {
            console.log('_removePreexistingKeys1', { keys });
            if (err) {
                return cb(err);
            }
            if (keys.length === 0) {
                return cb();
            }
            return redisClient.batch([['del', ...keys]], (err, res) => {
                console.log('_removePreexistingKeys2', { err, res });
                if (err) {
                    return cb(err);
                }
                const [cmdErr] = res[0];
                if (cmdErr) {
                    return cb(cmdErr);
                }
                return cb();
            });
        });
    }

    /**
     * Attempt to set the Redis hash, using an exponential backoff should the
     * key set fail. If the backoff time is exceeded, push the entry back into
     * the retry entry topic for a later reattempt.
     * @param {Object} data - The field and value for the Redis hash
     * @param {Object} kafkaEntry - The entry from the retry topic
     * @param {Werelogs} log - The werelogs logger
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _setRedisKey(data, kafkaEntry, log, cb) {
        this._backbeatTask.retry({
            actionDesc: 'set redis key',
            logFields: {},
            actionFunc: done => this._setRedisKeyOnce(data, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, err => {
            if (err && err.retryable === true) {
                log.info('publishing entry back into the kafka queue');
                const entry = Buffer.from(kafkaEntry.value).toString();
                return this._failedCRRProducer.publishFailedCRREntry(entry, cb);
            }
            log.info('successfully set redis key');
            return cb();
        });
    }

    /**
     * Attempt to set the Redis hash.
     * @param {Object} data - The key and value for the Redis key
     * @param {Werelogs} log - The werelogs logger
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _setRedisKeyOnce(data, log, cb) {
        const { key, value } = data;
        const expiry = this._repConfig.monitorReplicationFailureExpiryTimeS;
        const cmd = ['set', key, value, 'EX', expiry];
        return redisClient.batch([cmd], (err, res) => {
            if (err) {
                return cb({ retryable: true });
            }
            const [cmdErr] = res[0];
            return cb({ retryable: cmdErr !== null });
        });
    }
}

module.exports = FailedCRRConsumer;
