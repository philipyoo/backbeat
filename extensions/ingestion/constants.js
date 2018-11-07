'use strict'; // eslint-disable-line

const testIsOn = process.env.CI === 'true';

const constants = {
    zookeeperIngestionNamespace:
        testIsOn ? '/backbeattest/ingestion' : '/backbeat/ingestion',
    zkStatePath: '/state',
};

module.exports = constants;
