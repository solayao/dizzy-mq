const {Redis} = require('dizzyl-util/es/dbs');
const redisServer = new Redis({
    "host": "127.0.0.1",
    "port": 6379
});
const {PENDINGKEY, DOINGKEY} = require('./const');

const mqAdd = async (mess, cb) => {
    await redisServer.actionForClient(client => client.RPUSHAsync(PENDINGKEY, mess));
    cb();
}

const mqDoing = async (mess) => {
    // const mess = await redisServer.actionForClient(client => client.BLPOPAsync(PENDINGKEY, 30));
    if (mess) {
        await redisServer.hmSet(DOINGKEY, {
            [mess]: JSON.stringify({
                timestamp: new Date().valueOf(),
                key: mess
            })
        });
    }
    return;
}

const mqAck = async (mess, cb) => {
    let messVal = await redisServer.actionForClient(client => client.HGETAsync(DOINGKEY, mess));
    let messValue = JSON.parse(messVal);
    await redisServer.actionForClient(client => client.HDELAsync(DOINGKEY, mess));
    if (new Date().valueOf() - messValue.timestamp > 60 * 1000) {
        await redisServer.actionForClient(client => client.RPUSHAsync(PENDINGKEY, mess));
    } else {
        return;
    }

    // cb();
}

module.exports = {
    mqAdd,
    mqDoing,
    mqAck
}