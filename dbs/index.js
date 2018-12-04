const {Redis, Mongo} = require('dizzyl-util/es/dbs');
const {redisConfig, mongoConfig} = require('./config.json');
const {redisConfig: redisConfigTest, mongoConfig: mongoConfigTest} = require('./config.test.json');

const isPro = process.env.NODE_ENV === 'production';
const redisServer = new Redis(isPro ? redisConfig : redisConfigTest);
const mongoServer = new Mongo(isPro ? mongoConfig : mongoConfigTest);

process.on('exit', () => {
    redisServer.close();
    mongoServer.close();
});

module.exports = {
    redisServer,
    mongoServer,
};
