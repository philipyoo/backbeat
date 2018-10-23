'use strict'; // eslint-disable-line strict

const werelogs = require('werelogs');

const runServer = require('./lib/api/BackbeatServer');

const Config = process.env.CI !== 'true' ? require('./conf/Config') :
    require('./tests/config.json');

if (process.env.CI === 'true' && process.env.CI_AUTH_TYPE === 'role') {
    Config.extensions.replication.source.auth.type = 'role';
    delete Config.extensions.replication.source.auth.account;
}

const Logger = werelogs.Logger;

werelogs.configure({
    level: Config.log.logLevel,
    dump: Config.log.dumpLevel,
});

runServer(Config, Logger);
