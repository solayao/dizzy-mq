const {redisServer} = require('../dbs');
const {PENDINGKEY, DOINGKEY, MQKEYJOIN} = require('./const');
const schedule = require('node-schedule');

const mqAdd = async (mess) => {
    await redisServer.actionForClient(client => client.RPUSHAsync(PENDINGKEY, mess));
}

const mqDoing = async (mess) => {
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

const mqAck = async (mess) => {
    let messVal = await redisServer.actionForClient(client => client.HGETAsync(DOINGKEY, mess));
    let messValue = JSON.parse(messVal);
    await redisServer.actionForClient(client => client.HDELAsync(DOINGKEY, mess));
    if (new Date().valueOf() - messValue.timestamp > 60 * 1000) {
        await redisServer.actionForClient(client => client.RPUSHAsync(PENDINGKEY, mess));
    } else {
        return;
    }
}

const mqCheckPend = (io, ioSocket) =>{
    let j = schedule.scheduleJob('*/10 * * * * *', async () => {
        const mess = await redisServer.actionForClient(client => client.BLPOPAsync(PENDINGKEY, 30));
        const mqKey = mess ? mess[1] : null;
        if (!mqKey) return;
        const mqKeyList = mqKey.split(MQKEYJOIN);
        const socketId = mqKeyList[0], mqName = mqKeyList[1], mqParam = mqKeyList[2];
        if (mqName === 'start-crawler-ch') {
            io.to('crawler').clients((error, clients) => {
                if (error) throw error;
                if (clients[0]) {
                    ioSocket[clients[0]].emit(mqName, mqParam, mqKey, mqDoing);
                } else {
                    mqAdd(mess);
                }
            });
        }
    });   

    process.on('exit', (code) => {
        j.cancel();
    });
}

module.exports = {
    mqAdd,
    mqDoing,
    mqAck,
    mqCheckPend
}