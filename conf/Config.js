'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');

const fs = require('fs');
const path = require('path');
const joi = require('joi');

const extensions = require('../extensions');
const backbeatConfigJoi = require('./config.joi.js');
const monitoringClient = require('../lib/clients/monitoringHandler');

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

class Config extends EventEmitter {
    constructor() {
        super();
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the BACKBEAT_CONFIG_FILE environment var.
         */
        this._basePath = __dirname;
        if (process.env.BACKBEAT_CONFIG_FILE !== undefined) {
            this._configPath = process.env.BACKBEAT_CONFIG_FILE;
        } else {
            this._configPath = path.join(this._basePath, 'config.json');
        }

        let config;
        try {
            const data = fs.readFileSync(this._configPath,
                                         { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse config file: ${err.message}`);
        }

        const parsedConfig = joi.attempt(config, backbeatConfigJoi,
                                         'invalid backbeat config');

        if (parsedConfig.extensions) {
            Object.keys(parsedConfig.extensions).forEach(extName => {
                const index = extensions[extName];
                if (!index) {
                    throw new Error(`configured extension ${extName}: ` +
                                    'not found in extensions directory');
                }
                if (index.configValidator) {
                    const extConfig = parsedConfig.extensions[extName];
                    const validatedConfig =
                              index.configValidator(this, extConfig);
                    parsedConfig.extensions[extName] = validatedConfig;
                }
            });
        }

        if (parsedConfig.extensions && parsedConfig.extensions.replication
            && parsedConfig.extensions.replication.destination
            && parsedConfig.extensions.replication.destination.bootstrapList) {
            this.bootstrapList = parsedConfig.extensions.replication
            .destination.bootstrapList;
        } else {
            this.bootstrapList = [];
        }

        if (parsedConfig.extensions && parsedConfig.extensions.ingestion
            && parsedConfig.extensions.ingestion.sources) {
            this.ingestionSourceList =
                parsedConfig.extensions.ingestion.sources;
        } else {
            this.ingestionSourceList = [];
        }

        // whitelist IP, CIDR for health checks
        const defaultHealthChecks = ['127.0.0.1/8', '::1'];
        const healthChecks = parsedConfig.server.healthChecks;
        healthChecks.allowFrom =
            healthChecks.allowFrom.concat(defaultHealthChecks);

        if (parsedConfig.redis &&
          typeof parsedConfig.redis.sentinels === 'string') {
            const redisConf = { sentinels: [], name: parsedConfig.redis.name };
            parsedConfig.redis.sentinels.split(',').forEach(item => {
                const [host, port] = item.split(':');
                redisConf.sentinels.push({ host,
                    port: Number.parseInt(port, 10) });
            });
            parsedConfig.redis = redisConf;
        }

        // default to standalone configuration if sentinel not setup
        if (!parsedConfig.redis || !parsedConfig.redis.sentinels) {
            this.redis = Object.assign({}, parsedConfig.redis,
                { host: '127.0.0.1', port: 6379 });
        }

        if (parsedConfig.localCache) {
            this.localCache = {
                host: config.localCache.host,
                port: config.localCache.port,
                password: config.localCache.password,
            };
        }

        // additional certs checks
        if (parsedConfig.certFilePaths) {
            parsedConfig.https = this._parseCertFilePaths(
                parsedConfig.certFilePaths);
        }
        if (parsedConfig.internalCertFilePaths) {
            parsedConfig.internalHttps = this._parseCertFilePaths(
                parsedConfig.internalCertFilePaths);
        }
        // config is validated, safe to assign directly to the config object
        Object.assign(this, parsedConfig);

        this.transientLocations = {};
    }

    _parseCertFilePaths(certFilePaths) {
        const { key, cert, ca } = certFilePaths;

        const makePath = value =>
              (value.startsWith('/') ?
               value : `${this._basePath}/${value}`);
        const https = {};
        if (key && cert) {
            const keypath = makePath(key);
            const certpath = makePath(cert);
            fs.accessSync(keypath, fs.F_OK | fs.R_OK);
            fs.accessSync(certpath, fs.F_OK | fs.R_OK);
            https.cert = fs.readFileSync(certpath, 'ascii');
            https.key = fs.readFileSync(keypath, 'ascii');
        }
        if (ca) {
            const capath = makePath(ca);
            fs.accessSync(capath, fs.F_OK | fs.R_OK);
            https.ca = fs.readFileSync(capath, 'ascii');
        }
        return https;
    }

    getBasePath() {
        return this._basePath;
    }

    getConfigPath() {
        return this._configPath;
    }

    setBootstrapList(locationConstraints) {
        this.bootstrapList = Object.keys(locationConstraints).map(key => ({
            site: key,
            type: locationTypeMatch[locationConstraints[key].locationType],
        }));
        this.emit('bootstrap-list-update');
    }

    getBootstrapList() {
        monitoringClient.crrSiteCount.set(this.bootstrapList.length);
        return this.bootstrapList;
    }

    /**
     * add new ingestion source
     * @param {Object} source - source object
     * @param {String} source.name - source name
     * @param {String} source.prefix - source prefix
     * @return {undefined}
     */
    addIngestionSourceItem(source) {
        // NOTE: need to make sure sync add and source doesn't already exist
        const newSource = {
            name: source.name,
            prefix: source.prefix,
            cronRule: '*/5 * * * * *',
            zookeeperSuffix: `/${source.name}`,
            host: 'localhost',
            port: 8000,
            https: false,
            type: 'scality',
            raftCount: 8,
        };
        this.ingestionSourceList.push(newSource);
        this.emit('ingestion-source-list-update');
    }

    /**
     * remove ingestion source
     * @param {Object} source - source object
     * @param {String} source.name - source name
     * @param {String} source.prefix - source prefix
     * @return {undefined}
     */
    removeIngestionSourceItem(source) {
        const index = this.ingestionSourceList.findIndex(s =>
            s.name === source.name && s.prefix === source.prefix);
        if (index !== -1) {
            this.ingestionSourceList.splice(index, 1);
        }
        this.emit('ingestion-source-list-update');
    }

    getIngestionSourceList() {
        return this.ingestionSourceList;
    }

    setIsTransientLocation(locationName, isTransient) {
        this.transientLocations[locationName] = isTransient;
    }

    getIsTransientLocation(locationName) {
        return this.transientLocations[locationName] || false;
    }
}

module.exports = new Config();
