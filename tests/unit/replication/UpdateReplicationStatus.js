const assert = require('assert');

const UpdateReplicationStatus =
    require('../../../extensions/replication/tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const kafkaEntry = require('../../utils/kafkaEntry');

function getCompletedEntry() {
    return QueueEntry.createFromKafkaEntry(kafkaEntry)
        .toCompletedEntry('sf')
        .toCompletedEntry('replicationaws')
        .setSite('sf');
}

function getRefreshedEntry() {
    return QueueEntry.createFromKafkaEntry(kafkaEntry).setSite('sf');
}

function checkReplicationInfo(site, status, updatedSourceEntry) {
    const versionId =
        updatedSourceEntry.getReplicationSiteDataStoreVersionId(site);
    assert.strictEqual(
        updatedSourceEntry.getReplicationSiteStatus(site), status);
    assert.strictEqual(
        updatedSourceEntry.getReplicationSiteDataStoreVersionId(site),
        versionId);
}

describe('update replication status', () => {
    const rspMock = {
        getStateVars: () => ({
            repConfig: {
                replicationStatusProcessor: {},
            },
            sourceConfig: {
                auth: {},
            },
        }),
    };

    const updateReplicationStatus = new UpdateReplicationStatus(rspMock);

    it('should return a COMPLETED entry when metadata has not changed', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        const updatedSourceEntry = updateReplicationStatus
            ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        checkReplicationInfo('sf', 'COMPLETED', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });

    it('should return a PENDING entry when MD5 mismatch', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        refreshedEntry.setContentMd5('d41d8cd98f00b204e9800998ecf8427e');
        const updatedSourceEntry = updateReplicationStatus
            ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        checkReplicationInfo('sf', 'PENDING', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });

    it('should return a PENDING entry when tags mismatch', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        refreshedEntry.setTags({ key: 'value' });
        const updatedSourceEntry = updateReplicationStatus
            ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        checkReplicationInfo('sf', 'PENDING', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });
});
