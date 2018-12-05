const {redisServer} = require('../dbs');
const schedule = require('node-schedule');
const {isNotEmpty} = require('dizzyl-util/es/type');
const {PENDINGKEY, DOINGKEY, ERRORKEY, MQKEYJOIN,
    CHECKPENDSCHEDULESPE, CHECKDOINGSCHEDULESPE, HOURGETUPDATESCHEDULESPE,
    ROOMCRAWLERNAME, MQAUTO
} = require('./const');
const {STARTHOURUPDATE} = require('./taskName');
let checkPendSchedule = null, checkDoingSchedule = null;

const scheduleMess = (type, status) => {
    let s;
    if (status === -1)  s = '结束';
    else if (status === 0) s = '暂停';
    else s = '启动';

    console.log(`${s} ${type} 定时器任务.`);
}

process.on('exit', () => {
    if (checkPendSchedule) checkPendSchedule.cancel();
    if (checkDoingSchedule) checkDoingSchedule.cancel();
    scheduleMess('All', -1);
    checkPendSchedule = checkDoingSchedule = null;
});

const mqAdd = async (mess) => {
    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.RPUSHAsync(PENDINGKEY, ...mess) : 
            client.RPUSHAsync(PENDINGKEY, mess)
    );
    if (checkPendSchedule) checkPendSchedule.reschedule(CHECKPENDSCHEDULESPE);
    if (checkDoingSchedule) checkDoingSchedule.reschedule(CHECKDOINGSCHEDULESPE);
    scheduleMess('All', 1);
}

const mqDoing = async (mess) => {
    if (mess) {
        await redisServer.hmSet(DOINGKEY, {
            [mess]: JSON.stringify({
                timestamp: new Date().valueOf(),
            })
        });
    }
    return;
}

const mqAck = async (mess) => {
    await redisServer.actionForClient(client => 
        Array.isArray(mess) ? client.HDELAsync(DOINGKEY, ...mess) : 
            client.HDELAsync(DOINGKEY, mess)
    );
}

const mqError = async (mess) => {
    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.RPUSHAsync(ERRORKEY, ...mess) : 
            client.RPUSHAsync(ERRORKEY, mess)
    );
}

const mqCheckPend = (io, ioSocket) =>{
    checkPendSchedule = schedule.scheduleJob(CHECKPENDSCHEDULESPE, async () => {
        scheduleMess('CheckPend', 1);
        let mess = await redisServer.actionForClient(client => client.BLPOPAsync(PENDINGKEY, 7));
        let mqKey = mess ? mess[1] : null;
        if (!mqKey) {
            checkPendSchedule.cancelNext();
            scheduleMess('CheckPend', 0);
            return;
        }
        let mqKeyList = mqKey.split(MQKEYJOIN);
        let socketId = mqKeyList[0], mqName = mqKeyList[1], mqParam = JSON.parse(mqKeyList[2]);
        if (mqParam.hasOwnProperty('room')) {
            io.to(mqParam.room).clients((error, clients) => {
                if (error) throw error;
                if (clients[0]) {
                    ioSocket[clients[0]].emit(mqName, JSON.stringify(mqParam), mqKey, mqDoing);
                } else {
                    mqAdd(mqKey);
                }
            })
        } else if (mqParam.hasOwnProperty('socketId')) {
            
        }
        // if (mqName === 'start-crawler-ch') {
        //     io.to('crawler').clients((error, clients) => {
        //         if (error) throw error;
        //         if (clients[0]) {
        //             ioSocket[clients[0]].emit(mqName, mqParam, mqKey, mqDoing);
        //         } else {
        //             mqAdd(mqKey);
        //         }
        //     });
        // }
        mess = mqKeyList = null;
        scheduleMess('CheckPend', -1);
    });
}

const mqCheckDoing = () => {
    checkDoingSchedule = schedule.scheduleJob(CHECKDOINGSCHEDULESPE, async () => {
        scheduleMess('CheckDoing', 1);
        let doingObj = await redisServer.hgetAll(DOINGKEY);
        if (isNotEmpty(doingObj)) {
            let nowTimestamp = new Date().valueOf();
            let timeoutKeyList = Object.keys(doingObj).filter(key => {
                let val = JSON.parse(doingObj[key]);
                return nowTimestamp - val.timestamp > 60 * 1000;
            });
            await mqAck(timeoutKeyList);
            await mqAdd(timeoutKeyList);
            nowTimestamp = timeoutKeyList = null;
        } else {
            checkDoingSchedule.cancelNext();
            scheduleMess('CheckDoing', 0);
            return;
        }
        doingObj = null;
        scheduleMess('CheckDoing', -1);
    });   
}

const mqGetHourUpdateTask = () => {
    schedule.scheduleJob(HOURGETUPDATESCHEDULESPE, () => {
        let param = {
            room: ROOMCRAWLERNAME
        }
        let taskName = MQAUTO+MQKEYJOIN+STARTHOURUPDATE+MQKEYJOIN+JSON.stringify(param);
        mqAdd(taskName);
        param = taskName = null;
    }); 
};

module.exports = {
    mqAdd,
    mqDoing,
    mqAck,
    mqError,
    mqCheckPend,
    mqCheckDoing,
    mqGetHourUpdateTask
}