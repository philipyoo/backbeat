'use strict'; // eslint-disable-line

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;
const MongoClient = require('arsenal').storage
    .metadata.mongoclient.MongoClientInterface;
const { usersBucket } = require('arsenal').constants;
const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const QueueEntry = require('../utils/QueueEntry');
const DeleteOpQueueEntry = require('../utils/DeleteOpQueueEntry');
const BucketQueueEntry = require('../utils/BucketQueueEntry');
const BucketMdQueueEntry = require('../utils/BucketMdQueueEntry');
const ObjectQueueEntry = require('../utils/ObjectQueueEntry');

/**
* Given that the largest object JSON from S3 is about 1.6 MB and adding some
* padding to it, Backbeat replication topic is currently setup with a config
* max.message.bytes.limit to 5MB. Consumers need to update their fetchMaxBytes
* to get atleast 5MB put in the Kafka topic, adding a little extra bytes of
* padding for approximation.
*/

// TODO: follow up with Jonathan.
// where is this topic limit set?
const CONSUMER_FETCH_MAX_BYTES = 5000020;

/**
 * @class MongoQueueProcessor
 *
 * @classdesc Background task that processes entries from the
 * generic kafka queue and pushes entries to mongo
 */
class MongoQueueProcessor {

    /**
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     * @param {Object} mongoProcessorConfig - mongo processor configuration
     *   object
     * @param {String} mongoProcessorConfig.topic - topic name
     * @param {String} mongoProcessorConfig.groupId - kafka
     *   consumer group ID
     * @param {Integer} mongoProcessorConfig.retryTimeoutS -
     *   number of seconds before giving up retries of an entry status
     *   update
     * @param {Object} mongoClientConfig - config for connecting to mongo
     */
    constructor(zkConfig, mongoProcessorConfig, mongoClientConfig) {
        this.zkConfig = zkConfig;
        this.mongoProcessorConfig = mongoProcessorConfig;
        this._consumer = null;
        this.logger =
            new Logger('Backbeat:MongoProcessor');
        this.mongoClientConfig = mongoClientConfig;
        this.mongoClientConfig.logger = this.logger;
        this._mongoClient = new MongoClient(this.mongoClientConfig);
    }

    /**
     * Start kafka consumer
     *
     * @return {undefined}
     */
    start() {
        this.logger.info('starting mongo queue processor');
        this._mongoClient.setup(err => {
            if (err) {
                this.logger.error('could not connect to MongoDB', { err });
            }
        });
        this._consumer = new BackbeatConsumer({
            // zookeeper: { connectionString: this.zkConfig.connectionString },
            topic: this.mongoProcessorConfig.topic,
            groupId: `${this.mongoProcessorConfig.groupId}36`,
            // Must always have concurrency of 1 so writes are in order
            kafka: { hosts: '127.0.0.1:9092' },
            concurrency: 1,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        this._consumer.on('error', err => {
            this.logger.error('ERR', { err });
        });
        this._consumer.subscribe();
        this.logger.info('mongo queue processor is ready to consume');
    }

    /**
     * Stop kafka consumer and commit current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (!this._consumer) {
            return setImmediate(done);
        }
        return this._consumer.close(done);
    }

    /**
     * Put kafka queue entry into mongo
     *
     * @param {object} kafkaEntry - entry generated by generic populator
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.logger.error('error processing source entry',
                              { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        if (sourceEntry instanceof DeleteOpQueueEntry) {
            const bucket = sourceEntry.getBucket();
            const key = sourceEntry.getObjectVersionedKey();
            // Always call deleteObject with version params undefined so
            // that mongoClient will use deleteObjectNoVer which just deletes
            // the object without further manipulation/actions.
            // S3 takes care of the versioning logic so consuming the queue
            // is sufficient to replay the version logic in the consumer.
            return this._mongoClient.deleteObject(bucket, key, undefined,
                this.logger, err => {
                    if (err) {
                        this.logger.error('error deleting object from mongo',
                        { error: err });
                        return done(err);
                    }
                    this.logger.info('successfully deleted object from mongo',
                    { key });
                    return done();
                });
        }
        if (sourceEntry instanceof ObjectQueueEntry) {
            const bucket = sourceEntry.getBucket();
            // always use versioned key so putting full version state to mongo
            const key = sourceEntry.getObjectVersionedKey();
            const objVal = sourceEntry.getValue();

            // Always call putObject with version params undefined so
            // that mongoClient will use putObjectNoVer which just puts
            // the object without further manipulation/actions.
            // S3 takes care of the versioning logic so consuming the queue
            // is sufficient to replay the version logic in the consumer.
            return this._mongoClient.putObject(bucket, key, objVal, undefined,
                this.logger, err => {
                    if (err) {
                        this.logger.error('error putting object to mongo',
                        { error: err });
                        return done(err);
                    }
                    this.logger.info('successfully put object to mongo',
                    { key });
                    return done();
                });
        }
        if (sourceEntry instanceof BucketMdQueueEntry) {
            const masterBucket = sourceEntry.getMasterBucket();
            const instanceBucket = sourceEntry.getInstanceBucket();
            const val = sourceEntry.getValue();
            return this._mongoClient.putObject(masterBucket,
                instanceBucket, val, undefined,
                this.logger, err => {
                    if (err) {
                        this.logger.error('error putting bucket ' +
                        'metadata to mongo',
                        { error: err, masterBucket, instanceBucket });
                        return done(err);
                    }
                    this.logger.info('successfully put bucket' +
                    ' metadata to mongo', { masterBucket, instanceBucket });
                    return done();
                });
        }
        if (sourceEntry instanceof BucketQueueEntry) {
            const bucketOwnerKey = sourceEntry.getBucketOwnerKey();
            const val = sourceEntry.getValue();
            return this._mongoClient.putObject(usersBucket,
                bucketOwnerKey, val, undefined,
                this.logger, err => {
                    if (err) {
                        this.logger.error('error putting to usersbucket',
                        { error: err, bucketOwnerKey });
                        return done(err);
                    }
                    this.logger.info('successfully put to usersBucket',
                    { bucketOwnerKey });
                    return done();
                });
        }
        this.logger.warn('skipping unknown source entry',
                            { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }

    /**
     * Bootstrap kafka consumer by periodically sending bootstrap
     * messages and wait until it's receiving newly produced messages
     * in a timely fashion. ONLY USE FOR TESTING PURPOSE.
     *
     * @param {function} cb - callback when consumer is effectively
     * receiving newly produced messages
     * @return {undefined}
     */
    bootstrapKafkaConsumer(cb) {
        this._consumer.bootstrap(cb);
    }
}

module.exports = MongoQueueProcessor;
