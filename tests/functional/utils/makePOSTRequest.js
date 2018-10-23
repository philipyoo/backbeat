const http = require('http');

const config = require('../../config.json');

function makePOSTRequest(options, body, cb) {
    const req = http.request(options, res => cb(null, res));
    req.on('error', err => cb(err));
    req.end(body);
}

function getResponseBody(res, cb) {
    res.setEncoding('utf8');
    const resBody = [];
    res.on('data', chunk => resBody.push(chunk));
    res.on('end', () => cb(null, resBody.join('')));
    res.on('error', err => cb(err));
}

function makeRetryPOSTRequest(body, cb) {
    const options = {
        host: config.server.host,
        port: config.server.port,
        method: 'POST',
        path: '/_/crr/failed',
    };
    makePOSTRequest(options, body, cb);
}

module.exports = { makePOSTRequest, getResponseBody, makeRetryPOSTRequest };
