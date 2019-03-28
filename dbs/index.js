const {Redis} = require('dizzyl-util/es/dbs');
const {redisConfig} = require('./config.json');

const isPro = process.env.NODE_ENV === 'production';
const proRedis = process.env.REDIS_CONFIG;
// const proMongo = process.env.MONGO_CONFIG;
const redisServer = new Redis(isPro && proRedis ? JSON.parse(proRedis) : redisConfig);
// const mongoServer = new Mongo(isPro && proMongo ? JSON.parse(proMongo) : mongoConfig);

process.on('exit', () => {
    redisServer.close();
    // mongoServer.close();
});

module.exports = {
    redisServer,
    // mongoServer,
};
