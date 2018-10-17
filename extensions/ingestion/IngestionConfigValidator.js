const joi = require('joi');

const sourceItemJoi = joi.array().items(
    joi.object({
        sourceBucketName: joi.string().required,
        zenkoBucketName: joi.string().required,
        locationType: joi.string().required,
        locationName: joi.string().required,
        host: joi.string().required,
        port: joi.number().greater(0).required(),
        transport: joi.string.required,
    })
).default([]);

const joiSchema = {
    auth: joi.object({
        type: joi.alternatives().try('account', 'service', 'vault')
            .required(),
        account: joi.string()
            .when('type', { is: 'account', then: joi.required() })
            .when('type', { is: 'service', then: joi.required() }),
        vault: joi.object({
            host: joi.string().required(),
            port: joi.number().greater(0).required(),
            adminPort: joi.number().greater(0)
                .when('adminCredentialsFile', {
                    is: joi.exist(),
                    then: joi.required(),
                }),
            adminCredentialsFile: joi.string().optional(),
        }).when('type', { is: 'vault', then: joi.required() }),
    }).required(),
    topic: joi.string().required(),
    zookeeperPath: joi.string().required(),
    cronRule: joi.string().default('*/5 * * * * *'),
    sources: sourceItemJoi,
};

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
