const arsenal = require('arsenal');
const Logger = require('werelogs').Logger;

const { MongoClientInterface } = arsenal.storage.metadata.mongoclient;
const errors = arsenal.errors;

const USERSBUCKET = arsenal.constants.usersBucket;
const PENSIEVE = 'PENSIEVE';
const METASTORE = '__metastore';

class MongoMDWrapper extends MongoClientInterface {
    constructor(params) {
        super(Object.assign({}, params, {
            logger: new Logger('Backbeat:MongoMDWrapper'),
        }));
    }

    getIngestionBuckets(callback) {
        const m = this.getCollection(METASTORE);
        // eslint-disable-next-line
        m.find({
            _id: {
                $nin: [PENSIEVE, USERSBUCKET],
            },
            'value.ingestion': {
                $exists: true,
            },
        }, {
            'value.name': true,
            'value.ingestion': true,
            'value.locationConstraint': true,
        }).toArray((err, doc) => {
            if (err) {
                this.logger.error(
                    'getIngestionBuckets: error getting ingestion buckets',
                    { error: err.message });
                return callback(errors.InternalError);
            }
            if (!doc) {
                return callback(errors.NoSuchKey);
            }
            return callback(null, doc.value);
        });
    }
}

module.exports = MongoMDWrapper;
