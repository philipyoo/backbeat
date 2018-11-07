'use strict'; // eslint-disable-line

const async = require('async');

const Logger = require('werelogs').Logger;

const zookeeper = require('../../lib/clients/zookeeper');
const { zkStatePath, zookeeperIngestionNamespace } = require('./constants');

// Manage zookeeper state
// On startup, start all prior ingestion buckets saved in zk
// On startup, if encountering any new ingestion buckets on startup, populate
//   those as well
// While active, keep ingestion locations in memory

// General idea is to reduce zookeeper reads and hold state in-memory
// Any time a write occurs, update the in-memory state

class IngestionStateManager {
    constructor(zkConfig) {
        this._zkConfig = zkConfig;
        this._log = new Logger('Backbeat:IngestionStateManager');

        this._state = {};

        this._zkClient = null;
        this._ingestionSources = null;
    }

    setup(cb) {
        const { connectionString, autoCreateNamespace } = this._zkConfig;
        this._log.info('opening zookeeper connection', {
            zookeeperUrl: connectionString,
        });
        const zkClient = zookeeper.createClient(connectionString, {
            autoCreateNamespace,
        });
        zkClient.connect();
        zkClient.once('error', err => {
            this._log.fatal('error connecting to zookeeper', {
                error: err.message,
            });
            return cb(err);
        });
        zkClient.once('ready', () => {
            zkClient.removeAllListeners('error');
            const path = this._getStateZkPath();
            zkClient.mkdirp(path, err => {
                if (err) {
                    this._log.fatal('could not create path in zookeeper', {
                        method: 'IngestionStateManager.setup',
                        zookeeperPath: path,
                        error: err.message,
                    });
                    return cb(err);
                }
                this._zkClient = zkClient;
                return this._initState(cb);
            });
        });
    }

    _initState(cb) {
        const path = this._getStateZkPath();
        this._zkClient.getChildren(path, (err, children) => {
            if (err) {
                this._log.fatal('could not get zookeeper state', {
                    method: 'IngestionStateManager._initState',
                    zookeeperPath: path,
                    error: err.message,
                });
                return cb(err);
            }
            return async.each(children, (child, next) => {
                const locationPath = `${path}/${child}`;
                this._zkClient.getData(locationPath, (err, data) => {
                    if (err) {
                        this._log.fatal('could not get zookeeper child state', {
                            method: 'IngestionStateManager._initState',
                            zookeeperPath: locationPath,
                            error: err.message,
                        });
                        return next(err);
                    }
                    this._state[child] = JSON.parse(data.toString());
                    return next();
                });
            }, cb);
        });
    }

    _getStateZkPath() {
        return `${zookeeperIngestionNamespace}${zkStatePath}`;
    }

    getLocationState(location) {
        return this._state[location];
    }

    /**
     * Update state for a given location both in-memory and in zookeeper
     * @param {String} location - location to update
     * @param {Object} stateChange - new state changes
     * @param {Function} cb - callback(error, newState)
     * @return {undefined}
     */
    updateLocationState(location, stateChange, cb) {
        const newState = Object.assign(
            {}, this._state[location] || {}, stateChange);
        const path = `${this._getStateZkPath()}/${location}`;
        const bufferedData = Buffer.from(JSON.stringify(newState));
        this._zkClient.setData(path, bufferedData, err => {
            if (err) {
                this._log.error('failed to set new zookeeper state', {
                    method: 'IngestionStateManager.updateLocationState',
                    zookeeperPath: path,
                    error: err,
                });
                return cb(err);
            }
            return cb(null, newState);
        });
    }
}

module.export = IngestionStateManager;
