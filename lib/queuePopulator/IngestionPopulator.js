const async = require('async');

const Logger = require('werelogs').Logger;

const zookeeper = require('../clients/zookeeper');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const IngestionReader = require('./IngestionReader');
const BucketFileLogReader = require('./BucketFileLogReader');
const MetricsProducer = require('../MetricsProducer');
const MetricsConsumer = require('../MetricsConsumer');
const MongoLogReader = require('./MongoLogReader');
const IngestionQueuePopulator =
    require('../../extensions/ingestion/IngestionQueuePopulator');

class IngestionPopulator {
    /**
     * Create an ingestion populator to populate various kafka queues
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper
     *   connection string as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - kafka hosts list
     * as "host:port[,host:port...]"
     * @param {Object} qpConfig - queue populator configuration
     * @param {String} qpConfig.zookeeperPath - sub-path to use for
     *   storing populator state in zookeeper
     * @param {String} qpConfig.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [qpConfig.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [qpConfig.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Object} [qpConfig.mongo] - mongo source
     *   configuration (mandatory if logSource is "mongo")
     * @param {Object} mConfig - metrics configuration object
     * @param {string} mConfig.topic - metrics topic
     * @param {Object} rConfig - redis configuration object
     * @param {Object} extConfigs - configuration of extensions: keys
     *   are extension names and values are extension's config object.
     * @param {Object} s3Config - configuration to connect to S3 Client
     */
    constructor(zkConfig, kafkaConfig, qpConfig, mConfig, rConfig,
                extConfigs, s3Config) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.qpConfig = qpConfig;
        this.mConfig = mConfig;
        this.rConfig = rConfig;
        this.extConfigs = extConfigs;
        this.s3Config = s3Config;

        this.log = new Logger('Backbeat:IngestionPopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = [];
        // metrics clients
        this._mProducer = null;
        this._mConsumer = null;

        // list of provisioned log sources
        this._provisionedIngestionSources = {};
    }

    /**
     * Open the ingestion populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        // TODO-FIX: To support dynamic updates and avoid race conditions,
        //           we should initialize necessary internal components and
        //           individually add log sources from bin/ingestion.js
        this._loadExtension();
        async.series([
            next => this._setupMetricsClients(err => {
                if (err) {
                    this.log.error('error setting up metrics client', {
                        method: 'IngestionPopulator.open',
                        error: err,
                    });
                }
                return next(err);
            }),
            next => this._setupIngestionExtension(err => {
                if (err) {
                    this.log.error(
                        'error setting up ingestion extension', {
                            method: 'IngestionPopulator.open',
                            error: err,
                        });
                }
                return next(err);
            }),
            next => this._setupZookeeper(err => {
                if (err) {
                    return next(err);
                }
                this._setupLogSources();
                return next();
            }),
        ], err => {
            if (err) {
                this.log.error('error starting up ingestion populator',
                    { method: 'IngestionPopulator.open',
                        error: err });
                return cb(err);
            }
            return cb();
        });
    }

    _setupMetricsClients(cb) {
        // Metrics Consumer
        this._mConsumer = new MetricsConsumer(this.rConfig, this.mConfig,
            this.kafkaConfig);
        this._mConsumer.start();

        // Metrics Producer
        this._mProducer = new MetricsProducer(this.kafkaConfig, this.mConfig);
        this._mProducer.setupProducer(cb);
    }

    /**
     * Close the queue populator
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return this._closeLogState(cb);
    }

    _setupLogSources() {
        // switch (this.qpConfig.logSource) {
        // case 'bucketd':
        //     // initialization of log source is deferred until the
        //     // dispatcher notifies us of which raft sessions we're
        //     // responsible for
        //     this._subscribeToLogSourceDispatcher('raft-id-dispatcher');
        //     break;
        // case 'dmd':
        //     this.logReadersUpdate = [
        //         new BucketFileLogReader({ zkClient: this.zkClient,
        //             kafkaConfig: this.kafkaConfig,
        //             dmdConfig: this.qpConfig.dmd,
        //             logger: this.log,
        //             extensions: this._extensions,
        //             metricsProducer: this._mProducer,
        //         }),
        //     ];
        //     break;
        // default:
        //     throw new Error("bad 'logSource' config value: expect 'bucketd,'" +
        //                 ` mongo or 'dmd', got '${this.qpConfig.logSource}'`);
        // }
        //
        // TODO: Can use logSource on each source to support ring and other
        //       external sources like aws or gcp

        if (this.extConfigs.ingestion) {
            this.extConfigs.ingestion.sources.map(sourceObj => {
                const zookeeperUrlIngest =
                    `${this.zkConfig.connectionString}` +
                    `${this.extConfigs.ingestion.zookeeperPath}/` +
                    `${sourceObj.name}`;
                const zkEndpointIngest =
                    `${zookeeperUrlIngest}/raft-id-dispatcher`;
                this._subscribeToLogSourceDispatcher(zkEndpointIngest,
                    sourceObj, this.extConfigs.ingestion.zookeeperPath);
                return undefined;
            });
        }
    }

    // specifically for ingestion
    addNewLogSource(newSource, cb) {
        this._extension[0].createZkPath(err => {
            if (err) {
                return cb(err);
            }
            const zookeeperUrlIngest =
                `${this.zkConfig.connectionString}` +
                `${this.extConfigs.ingestion.zookeeperPath}/` +
                `${newSource.name}`;
            const zkEndpointIngest = `${zookeeperUrlIngest}/raft-id-dispatcher`;

            if (!this.raftIdDispatcher) {
                this._subscribeToLogSourceDispatcher(zkEndpointIngest,
                    newSource, this.extConfigs.ingestion.zookeeperPath);
            } else {
                // save "items" from ProvisionDispatcher as an instance var.
                // CR IngestionReader's for each of these items.
                // Add new IngestionReader's to `this.logReadersUpdate`
                // Add log.
            }
            return cb();
        }, newSource);
    }

    _subscribeToLogSourceDispatcher(zkEndpoint, bucketd, ingestionPath) {
        this.raftIdDispatcher =
            new ProvisionDispatcher({ connectionString: zkEndpoint });
        this.raftIdDispatcher.subscribe((err, items) => {
            if (err) {
                this.log.error(
                    'error when receiving log source provision list',
                    { zkEndpoint, error: err });
                return undefined;
            }
            if (items.length === 0) {
                this.log.info('no log source provisioned, idling',
                              { zkEndpoint });
            }
            const newRaftReaders = items.map(
                token => {
                    if (token === 'mongo') {
                        return new MongoLogReader({
                            zkClient: this.zkClient,
                            kafkaConfig: this.kafkaConfig,
                            zkConfig: this.zkConfig,
                            mongoConfig: this.qpConfig.mongo,
                            logger: this.log,
                            extensions: this._extension,
                            metricsProducer: this._mProducer,
                        });
                    }
                    return new IngestionReader({
                        zkClient: this.zkClient,
                        kafkaConfig: this.kafkaConfig,
                        bucketdConfig: bucketd,
                        raftId: token,
                        logger: this.log,
                        extensions: this._extension,
                        metricsProducer: this._mProducer,
                        ingestionPath,
                        qpConfig: this.qpConfig,
                        s3Config: this.s3Config,
                    });
                });
            newRaftReaders.forEach(reader =>
                this.logReadersUpdate.push(reader));
            return undefined;
        });
        this.log.info('waiting to be provisioned a log source',
                      { zkEndpoint });
    }

    _setupZookeeper(done) {
        const zookeeperUrl = this.zkConfig.connectionString;
        this.log.info('opening zookeeper connection for persisting ' +
                      'populator state',
                      { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this.zkConfig.autoCreateNamespace,
        });
        this.zkClient.connect();
        this.zkClient.once('error', done);
        this.zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this.zkClient.removeAllListeners('error');
            done();
        });
    }

    _loadExtension() {
        const ingestionExt = 'ingestion';
        const extConfig = this.extConfigs[ingestionExt];

        const index = require(`../../extensions/${ingestionExt}/index.js`);

        if (!index.queuePopulatorExtension) {
            this.log.fatal('Missing ingestion populator extension file');
            process.exit(1);
        }

        // eslint-disable-next-line new-cap
        const ext = new index.queuePopulatorExtension({
            config: extConfig,
            logger: this.log,
        });
        ext.setZkConfig(this.zkConfig);
        // use an array to fit LogReader model
        this._extension = [ext];

        this.log.info('ingestion extension is active');
    }

    _setupIngestionExtension(cb) {
        // dealing with a single extension
        this._extension[0].setupZookeeper(err => {
            if (err) {
                return cb(err);
            }
            return async.each(this.extConfigs.ingestion.sources,
                (source, next) => this._extension[0].createZkPath(next, source),
                cb);
        });
    }

    _setupUpdatedReaders(done) {
        const newReaders = this.logReadersUpdate;
        this.logReadersUpdate = [];
        async.each(newReaders, (logReader, cb) => logReader.setup(cb),
                   err => {
                       if (err) {
                           return done(err);
                       }
                       this.logReaders.push(...newReaders);
                       return done();
                   });
    }

    _closeLogState(done) {
        if (this.raftIdDispatcher !== undefined) {
            return this.raftIdDispatcher.unsubscribe(done);
        }
        return process.nextTick(done);
    }

    _processAllLogEntries(params, done) {
        return async.map(
            this.logReaders,
            (logReader, done) => logReader.processAllLogEntries(params, done),
            (err, results) => {
                if (err) {
                    return done(err);
                }
                const annotatedResults = results.map(
                    (result, i) => Object.assign(result, {
                        logSource: this.logReaders[i].getLogInfo(),
                        logOffset: this.logReaders[i].getLogOffset(),
                    }));
                return done(null, annotatedResults);
            });
    }

    processAllLogEntries(params, done) {
        if (this.logReadersUpdate.length >= 1) {
            return this._setupUpdatedReaders(err => {
                if (err) {
                    return done(err);
                }
                return this._processAllLogEntries(params, done);
            });
        }
        return this._processAllLogEntries(params, done);
    }

    zkStatus() {
        return this.zkClient.getState();
    }
}


module.exports = IngestionPopulator;
