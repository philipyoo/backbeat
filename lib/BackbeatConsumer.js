const { EventEmitter } = require('events');
const kafka = require('node-rdkafka');
const assert = require('assert');
const async = require('async');
const joi = require('joi');
const Logger = require('werelogs').Logger;
const zookeeper = require('node-zookeeper-client');

const zookeeperHelper = require('./clients/zookeeper');
const BackbeatProducer = require('./BackbeatProducer');

const replicationTopic = require('../conf/Config').extensions.replication.topic;
// controls the number of messages to process in parallel
const CONCURRENCY_DEFAULT = 1;
const CLIENT_ID = 'BackbeatConsumer';

const { withTopicPrefix } = require('./util/topic');

class BackbeatConsumer extends EventEmitter {

    /**
     * constructor
     * @param {Object} config - config
     * @param {string} config.topic - Kafka topic to subscribe to
     * @param {function} config.queueProcessor - function to invoke to process
     * an item in a queue
     * @param {string} config.groupId - consumer group id. Messages are
     * distributed among multiple consumers belonging to the same group
     * @param {Object} [config.zookeeper] - zookeeper endpoint config
     * (only needed if config.backlogMetrics is set)
     * @param {string} config.zookeeper.connectionString - zookeeper
     * connection string as "host:port[/chroot]" (only needed if
     * config.backlogMetrics is set)
     * @param {Object} config.kafka - kafka connection config
     * @param {string} config.kafka.hosts - kafka hosts list
     * as "host:port[,host:port...]"
     * @param {string} [config.fromOffset] - valid values latest/earliest/none
     * @param {number} [config.concurrency] - represents the number of entries
     * that can be processed in parallel
     * @param {number} [config.fetchMaxBytes] - max. bytes to fetch in a
     * fetch loop
     * @param {object} [config.backlogMetrics] - param object to
     * publish backlog metrics to zookeeper (disabled if param object
     * is not set)
     * @param {string} config.backlogMetrics.zkPath - zookeeper base
     * path to publish metrics to
     * @param {boolean} [config.backlogMetrics.intervalS=60] -
     * interval in seconds between iterations of backlog metrics
     * publishing task
     * @param {boolean} [config.bootstrap=false] - TEST ONLY: true to
     * bootstrap the consumer with test messages until it starts
     * consuming them
     */
    constructor(config) {
        super();

        const configJoi = {
            zookeeper: joi.object({
                connectionString: joi.string().required(),
            }).when('backlogMetrics', { is: joi.exist(),
                                        then: joi.required() }),
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string().required(),
            groupId: joi.string().required(),
            queueProcessor: joi.func(),
            fromOffset: joi.alternatives().try('latest', 'earliest', 'none'),
            autoCommit: joi.boolean().default(false),
            concurrency: joi.number().greater(0).default(CONCURRENCY_DEFAULT),
            fetchMaxBytes: joi.number(),
            backlogMetrics: {
                zkPath: joi.string().required(),
                intervalS: joi.number().default(60),
            },
            bootstrap: joi.boolean().default(false),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');

        const { zookeeper, kafka, topic, groupId, queueProcessor,
                fromOffset, autoCommit, concurrency, fetchMaxBytes,
                backlogMetrics, bootstrap } = validConfig;

        this._zookeeperEndpoint = zookeeper && zookeeper.connectionString;
        this._kafkaHosts = kafka.hosts;
        this._fromOffset = fromOffset;
        this._autoCommit = autoCommit;
        this._log = new Logger(CLIENT_ID);
        this._topic = withTopicPrefix(topic);
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency;
        this._fetchMaxBytes = fetchMaxBytes;
        this._backlogMetrics = backlogMetrics;
        this._bootstrap = bootstrap;

        this._processingQueue = null;
        this._messagesConsumed = 0;
        this._consumer = null;
        this._consumerReady = false;
        this._bootstrapping = false;
        this._zookeeper = null;
        this._zookeeperReady = false;
        this._publishOffsetsCronTimer = null;
        this._publishOffsetsCronActive = false;

        this._init();
        return this;
    }

    _init() {
        if (this._bootstrap) {
            this._consumerReady = true;
        } else {
            this._initConsumer();
        }
        if (this._backlogMetrics) {
            this._initZookeeperClient();
        } else {
            this._zookeeperReady = true;
        }
        process.nextTick(this._checkIfReady.bind(this));
    }

    _initConsumer() {
        const consumerParams = {
            'metadata.broker.list': this._kafkaHosts,
            'group.id': this._groupId,
            'enable.auto.commit': this._autoCommit,
            'offset_commit_cb': this._onOffsetCommit.bind(this),
        };
        const topicParams = {};
        if (this._fromOffset !== undefined) {
            topicParams['auto.offset.reset'] = this._fromOffset;
        }
        if (this._fetchMaxBytes !== undefined) {
            consumerParams['fetch.message.max.bytes'] = this._fetchMaxBytes;
        }
        this._consumer = new kafka.KafkaConsumer(consumerParams, topicParams);
        this._consumer.connect({ timeout: 10000 }, () => {
            const opts = {
                topic: withTopicPrefix('backbeat-sanitycheck'),
                timeout: 10000,
            };
            this._consumer.getMetadata(opts, err => {
                if (err) {
                    this.emit('error', err);
                }
            });
        });
        return this._consumer.once('ready', () => {
            this._consumerReady = true;
            this._checkIfReady();
        });
    }

    _initZookeeperClient() {
        this._zookeeper = zookeeperHelper.createClient(this._zookeeperEndpoint);
        this._zookeeper.connect();
        this._zookeeper.once('ready', () => {
            this._zookeeperReady = true;
            this._checkIfReady();
        });
    }

    _checkIfReady() {
        if (this._consumerReady && this._zookeeperReady) {
            if (this._bootstrap) {
                if (!this._bootstrapping) {
                    this._bootstrapConsumer();
                }
            } else {
                this._onReady();
            }
        }
    }

    _onReady() {
        this.emit('ready');
        if (this._backlogMetrics) {
            this._publishOffsetsCronTimer =
                setInterval(this._publishOffsetsCron.bind(this),
                            this._backlogMetrics.intervalS * 1000);
        }
    }

    isReady() {
        let zk = null;
        let cs = null;
        if (this._backlogMetrics) {
            zk = this._zookeeper &&
                this._zookeeper.getState().code ===
                    zookeeper.State.SYNC_CONNECTED.code;
        } else {
            zk = true;
        }

        if (this._bootstrap) {
            cs = true;
        } else {
            cs = this._consumer && this._consumer.isConnected();
        }
        return zk && cs;
    }

    /**
    * subscribe to messages from a topic
    * Once subscribed, the consumer does a fetch from the topic with new
    * messages. Each fetch loop can contain one or more messages, so the fetch
    * is paused until the current queue of tasks are processed. Once the task
    * queue is empty, the current offset is committed and the fetch is resumed
    * to get the next batch of messages
    * @param {Boolean} [paused] - optional field. If true, kafka consumer should
    *   not subscribe to its topic
    * @return {this} current instance
    */
    subscribe(paused) {
        if (!paused) {
            this._consumer.subscribe([this._topic]);
        } else {
            this._log.debug(`consumer is paused for topic ${this._topic}`);
        }

        this._sendCanary(() => {
            this.emit('canary');
        });

        this._processingQueue = async.queue(
            this._queueProcessor, this._concurrency);
        // when the task queue is empty, commit offset for all
        // consumed partitions and try consuming new messages right away
        this._processingQueue.drain = () => {
            // when auto-commit is set we don't have to commit
            // explicitly, so let's not do it for performance reasons
            if (!this._autoCommit) {
                this._consumer.commit();
            }
            this.emit('consumed', this._messagesConsumed);
            this._messagesConsumed = 0;

            this._tryConsume();
        };

        this._consumer.on('event.error', error => {
            // This is a bit hacky: the "broker transport failure"
            // error occurs when the kafka broker reaps the idle
            // connections every few minutes, and librdkafka handles
            // reconnection automatically anyway, so we ignore those
            // harmless errors (moreover with the current
            // implementation there's no way to access the original
            // error code, so we match the message instead).
            if (!['broker transport failure',
                  'all broker connections are down']
                .includes(error.message)) {
                this._log.error('consumer error', {
                    error,
                    topic: this._topic,
                });
                this.emit('error', error);
            }
        });

        // trigger initial consumption
        this._tryConsume();
        return this;
    }

    _tryConsume() {
        // use non-flowing mode of consumption to add some flow
        // control: explicit consumption of messages is required,
        // needs explicit polling until new messages become available.
        this._consumer.consume(this._concurrency, (err, entries) => {
            if (!err) {
                entries.forEach(entry => {
                    this._messagesConsumed++;
                    this._processingQueue.push(
                        entry, err => this._onEntryProcessingDone(err, entry));
                });
            }
            if (err || entries.length === 0) {
                // retry later to fetch new messages in case no one is
                // available yet in the message queue
                setTimeout(this._tryConsume.bind(this), 1000);
            }
        });
    }

    _onEntryProcessingDone(err, entry) {
        const { topic, partition, offset, key, timestamp } = entry;
        this._log.debug('finished processing of consumed entry', {
            method: 'BackbeatConsumer.subscribe',
            partition,
            offset,
        });
        if (err) {
            this._log.error('error processing an entry', {
                error: err,
                entry: { topic, partition, offset, key, timestamp },
            });
            this.emit('error', err, entry);
        }
    }

    _onOffsetCommit(err, topicPartitions) {
        if (err) {
            // NO_OFFSET is a "soft error" meaning that the same
            // offset is already committed, which occurs because of
            // auto-commit (e.g. if nothing was done by the producer
            // on this partition since last commit).
            if (err === kafka.CODES.ERRORS.ERR__NO_OFFSET) {
                return undefined;
            }
            this._log.error('error committing offsets to kafka',
                            { errorCode: err, topicPartitions });
        }
        this._log.debug('commit offsets callback',
                        { topicPartitions });
        return undefined;
    }

    _getOffsetZkPath(partition, offsetType) {
        const basePath = `${this._backlogMetrics.zkPath}/` +
                  `${this._topic}/${partition}`;
        return (offsetType === 'topic' ?
                `${basePath}/topic` :
                `${basePath}/consumers/${this._groupId}`);
    }

    _publishOffset(partition, offset, offsetType, done) {
        const zkPath = this._getOffsetZkPath(partition, offsetType);
        const zkData = Buffer.from(offset.toString());
        this._zookeeper.setOrCreate(zkPath, zkData, err => {
            if (err) {
                this._log.error(
                    'error publishing offset to zookeeper',
                    { zkPath, offset, offsetType, error: err.message });
            } else {
                this._log.debug('published offset to zookeeper', {
                    topic: this._topic,
                    partition,
                    offsetType,
                    offset,
                });
            }
            return done(err);
        });
    }

    _publishOffsetsCron(cb) {
        if (this._publishOffsetsCronActive) {
            // skipping
            if (cb) {
                return process.nextTick(cb);
            }
            return undefined;
        }
        let consumerOffsets;
        try {
            // NOTE: for an unknown reason, in some cases all
            // partitions are published but some do not have a set
            // consumer offset yet, so pre-filter here.
            consumerOffsets = this._consumer.position()
                .filter(p => p.offset !== undefined);
        } catch (err) {
            this._log.error('error getting consumer current offsets',
                            { errorCode: err });
            if (cb) {
                return process.nextTick(cb);
            }
            return undefined;
        }
        this._publishOffsetsCronActive = true;

        const topicOffsets = [];
        return async.each(consumerOffsets, (p, done) => {
            this._getLatestTopicOffset(p.partition, (err, topicOffset) => {
                if (err) {
                    this._log.error('error getting latest topic offset', {
                        topic: this._topic,
                        partition: p.partition,
                        topicOffset,
                        error: err, // kafka error does not have a
                        // message field
                    });
                    return done(err);
                }
                topicOffsets.push({ partition: p.partition,
                                    offset: topicOffset });
                return async.parallel([
                    done => this._publishOffset(p.partition, p.offset,
                                                'consumer', done),
                    done => this._publishOffset(p.partition, topicOffset,
                                                'topic', done),
                ], done);
            });
        }, err => {
            if (!err) {
                this._log.info(
                    'published consumer and topic offsets to zookeeper', {
                        topic: this._topic,
                        consumerOffsets,
                        topicOffsets,
                    });
            }
            this._publishOffsetsCronActive = false;
            if (cb) {
                // used for shutdown
                cb(err);
            }
        });
    }

    /**
     * Fetch latest consumable offset from topic
     *
     * @param {number} partition partition number to fetch latest
     * consumable offset from
     * @param {function} cb - callback: cb(err, offset)
     * @return {undefined}
     */
    _getLatestTopicOffset(partition, cb) {
        this._consumer.queryWatermarkOffsets(
            this._topic, partition, 10000, (err, offsets) => {
                if (err) {
                    return cb(err);
                }
                // high watermark is last message pushed and consumable
                return cb(null, offsets.highOffset);
            });
    }

    /**
     * get metadata from kafka topics
     * @param {object} params - call params
     * @param {string} params.topic - topic name
     * @param {number} params.timeout - timeout for the request
     * @param {function} cb - callback: cb(err, response)
     * @return {undefined}
     */
    getMetadata(params, cb) {
        this._consumer.getMetadata(params, cb);
    }

    /**
     * Bootstrap consumer by periodically sending bootstrap messages
     * and wait until it's receiving newly produced messages in a
     * timely fashion.
     * @return {undefined}
     */
    _bootstrapConsumer() {
        const self = this;
        let lastBootstrapId;
        let producer; // eslint-disable-line prefer-const
        let producerTimer; // eslint-disable-line prefer-const
        let consumerTimer; // eslint-disable-line prefer-const
        function consumeCb(err, messages) {
            if (err) {
                return undefined;
            }
            messages.forEach(message => {
                const bootstrapId = JSON.parse(message.value).bootstrapId;
                if (bootstrapId) {
                    self._log.info('bootstraping backbeat consumer: ' +
                                   'received bootstrap message',
                                   { bootstrapId });
                    if (bootstrapId === lastBootstrapId) {
                        self._log.info('backbeat consumer is bootstrapped');
                        clearInterval(producerTimer);
                        clearInterval(consumerTimer);
                        self._consumer.commit();
                        self._consumer.unsubscribe();
                        producer.close(() => {
                            self._bootstrapping = false;
                            self._onReady();
                        });
                    }
                }
            });
            return undefined;
        }
        assert.strictEqual(this._consumer, null);
        producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaHosts },
            topic: this._topic,
        });
        producer.on('ready', () => {
            producerTimer = setInterval(() => {
                lastBootstrapId = `${Math.round(Math.random() * 1000000)}`;
                const contents = `{"bootstrapId":"${lastBootstrapId}"}`;
                this._log.info('bootstraping backbeat consumer: ' +
                               'sending bootstrap message',
                               { contents });
                producer.send([{ key: 'bootstrap',
                                 message: contents }],
                              () => {});
                if (!this._consumer) {
                    setTimeout(() => {
                        this._bootstrapping = true;
                        this._initConsumer();
                        this._consumer.on('ready', () => {
                            this._consumer.subscribe([this._topic]);
                            consumerTimer = setInterval(() => {
                                this._consumer.consume(1, consumeCb);
                            }, 200);
                        });
                    }, 500);
                }
            }, 5000);
        });
    }

    /**
     * pause the kafka consumer
     * @param {string} site - name of site
     * @return {undefined}
     */
    pause(site) {
        // Use of KafkaConsumer#pause did not work. Using alternative
        // of unsubscribe/subscribe
        this._consumer.unsubscribe();
        this._log.debug(`paused consumer for location: ${site}`, {
            method: 'BackbeatConsumer.pause',
        });
    }

    /**
     * resume the kafka consumer
     * @param {string} site - name of site
     * @return {undefined}
     */
    resume(site) {
        // if not subscribed, then subscribe
        if (this._consumer.subscription().length === 0) {
            this._consumer.subscribe([this._topic]);
            this._log.debug(`resumed consumer for location: ${site}`, {
                method: 'BackbeatConsumer.resume',
            });
        }
    }

    /**
     * check if the kafka consumer is active or paused
     * @return {boolean} if false, consumer is paused
     */
    getServiceStatus() {
        const subscriptions = this._consumer.subscription();
        return subscriptions.length > 0;
    }

    /**
     * Helper method to wait for consumers to be assigned to partitions before
     * proceeding to send canary messages.
     * @param {number} wait - current accumulated wait time
     * @param {callback} cb - callback to invoke
     * @return {undefined}
     */
    _waitForAssignment(wait, cb) {
        setTimeout(() => {
            const assignments = this._consumer.assignments();
            if (assignments.length === 0) {
                if (wait > 50000) {
                    return cb(true);
                }
                return this._waitForAssignment(wait + 2000, cb);
            }
            return cb();
        }, 2000);
    }

    /**
     * On start, send canary (noop) messages to set partition offsets for this
     * consumer group. Necessary to avoid pause/resume bug where new locations
     * on pause will not be able to queue entries as no current offset to
     * determine number of entries (lag) to consume on resume.
     * @param {callback} cb - callback to invoke
     * @return {undefined}
     */
    _sendCanary(cb) {
        const crrTopic = withTopicPrefix(replicationTopic);
        if (this._topic !== crrTopic) {
            return process.nextTick(cb);
        }
        return this._waitForAssignment(0, err => {
            if (err) {
                this._log.debug('could not send canary on consumer init, ' +
                'waiting for consumer assignment to partition took too long', {
                    method: 'BackbeatConsumer._sendCanary',
                });
                return cb();
            }
            const topicDetails = this._consumer._metadata.topics
                .find(topic => topic.name === crrTopic);
            const entries = topicDetails.partitions.map(p => {
                const contents = '{"canary":true}';
                return {
                    key: 'canary',
                    message: contents,
                    partition: p.id,
                };
            });
            const producer = new BackbeatProducer({
                kafka: { hosts: this._kafkaHosts },
                topic: this._topic,
            });
            return producer.on('ready', () => {
                producer.send(entries, () => {
                    producer.close(cb);
                });
            });
        });
    }

    /**
     * force commit the current offset and close the client connection
     * @param {callback} cb - callback to invoke
     * @return {undefined}
     */
    close(cb) {
        if (this._publishOffsetsCronTimer) {
            clearInterval(this._publishOffsetsCronTimer);
            this._publishOffsetsCronTimer = null;
        }
        if (this._publishOffsetsCronActive) {
            return setTimeout(() => this.close(cb), 1000);
        }
        return async.series([
            next => {
                if (this._consumer) {
                    this._consumer.commit();
                }
                if (this._backlogMetrics) {
                    // publish offsets to zookeeper
                    return this._publishOffsetsCron(() => next());
                }
                return process.nextTick(next);
            },
            next => {
                if (this._zookeeper) {
                    this._zookeeper.close();
                }
                if (this._consumer) {
                    this._consumer.unsubscribe();
                    this._consumer.disconnect();
                    this._consumer.on('disconnected', () => next());
                } else {
                    process.nextTick(next);
                }
            },
        ], () => cb());
    }
}

module.exports = BackbeatConsumer;
