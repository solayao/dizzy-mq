const {redisServer, mongoServer} = require('../dbs');
const schedule = require('node-schedule');
const {isNotEmpty} = require('dizzyl-util/es/type');
const {
    PENDINGKEY, 
    DOINGKEY, 
    ERRORKEY, 
    MQKEYJOIN, 
    ROOMCRAWLERNAME, 
    MQAUTO, 
    CHECKPENDSCHEDULESPE, 
    CHECKDOINGSCHEDULESPE, 
    HOURGETUPDATESCHEDULESPE, 
    ZEROPOINTSCHEDULESPE
} = require( './const');
const {
    CWNORMALHOURUPDATE, 
    CWZEROPOINTUPDATE,
} = require( '../socketio/taskName');
const {SuccessConsole} = require('dizzyl-util/es/log/ChalkConsole');

let checkPendSchedule = null, checkPend = true,
    checkDoingSchedule = null, checkDoing = true,
    normalHourSchedule = null,
    zeroPointSchedule = null;

/** 
 * 退出取消定时任务 && 回收变量
 */
process.on('exit', () => {
    if (checkPendSchedule) {
        checkPendSchedule.cancel();
        checkPend = false;
    }
    if (checkDoingSchedule) {
        checkDoingSchedule.cancel();
        checkDoing = false;
    }
    if (normalHourSchedule) normalHourSchedule.cancel();
    if (zeroPointSchedule) zeroPointSchedule.cancel();
    scheduleMess('All', -1);
    checkPendSchedule = checkDoingSchedule = normalHourSchedule = zeroPointSchedule =null;
});

/**
 * @description 定时任务输出
 * @param {*} type 类型
 * @param {*} status 状态 [-1, 0, 1]
 */
const scheduleMess = (type, status) => {
    let s;
    if (status === -1)  s = '结束';
    else if (status === 0) s = '暂停';
    else if (status === 1) s = '启动';
    else if (status === 2) s = '重启';

    console.log(`${s} ${type} 定时器任务.`);
}

/**
 * @description 创建MQ的任务名称
 * @param {*} socketid
 * @param {*} taskName
 * @param {*} params
 * @returns
 */
const createMQTaskName = (socketid, taskName, params) => {
    return socketid+MQKEYJOIN+taskName+MQKEYJOIN+JSON.stringify(params);
}

/**
 * @description 重新启动检查任务
 */
const restartCheck = () => {
    if (checkPendSchedule && !checkPend) {
        checkPendSchedule.reschedule(CHECKPENDSCHEDULESPE);
        scheduleMess('CheckPend', 2);
    }
    if (checkDoingSchedule && !checkDoing) {
        checkDoingSchedule.reschedule(CHECKDOINGSCHEDULESPE);
        scheduleMess('CheckDoing', 2);
    }
}

/**
 * @description 添加优先任务
 * @param {String|Array} mess socket.id||taskName||JSON.stringify(paramObj)
 * paramObj = {room?, socketId?, ...}
 */
const mqAddFirst = async (mess) => {
    if (Array.isArray(mess) && !isNotEmpty(mess)) return ;

    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.LPUSHAsync(PENDINGKEY, ...mess) : 
            client.LPUSHAsync(PENDINGKEY, mess)
    );
    restartCheck();
}

/**
 * @description 添加任务
 * @param {String|Array} mess socket.id||taskName||JSON.stringify(paramObj)
 * paramObj = {room?, socketId?, ...}
 */
const mqAdd = async (mess) => {
    if (Array.isArray(mess) && !isNotEmpty(mess)) return ;

    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.RPUSHAsync(PENDINGKEY, ...mess) : 
            client.RPUSHAsync(PENDINGKEY, mess)
    );
    restartCheck();
}

/**
 * @description 处理正在处理的任务
 * @param {String} mess
 * @returns
 */
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

/**
 * @description 处理完成的任务
 * @param {String||Array} mess
 */
const mqAck = async (mess) => {
    if (Array.isArray(mess) && !isNotEmpty(mess)) return ;

    await redisServer.actionForClient(client => 
        Array.isArray(mess) ? client.HDELAsync(DOINGKEY, ...mess) : 
            client.HDELAsync(DOINGKEY, mess)
    );
}

/**
 * @description 处理失败的任务
 * @param {String||Array} mess
 */
const mqError = async (mess) => {
    if (Array.isArray(mess) && !isNotEmpty(mess)) return ;

    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.RPUSHAsync(ERRORKEY, ...mess) : 
            client.RPUSHAsync(ERRORKEY, mess)
    );
}

/**
 * @description 处理mq-pending
 * @param {String} mqKey
 * @param {*} io
 * @param {*} ioSocket
 */
const mqPendResolute = (mqKey, io, ioSocket) => new Promise(resolve => {
    let mqKeyList = mqKey.split(MQKEYJOIN);
    let socketId = mqKeyList[0], mqName = mqKeyList[1], mqParam = JSON.parse(mqKeyList[2]);
    if (mqParam.hasOwnProperty('room')) {
        io.to(mqParam.room).clients((error, clients) => {
            if (error) throw error;
            let opt = {
                title: 'MQ Task Start',
                pathName: __filename,
                message: mqKey
            }
            if (clients[0]) {
                ioSocket[clients[0]].emit(mqName, JSON.stringify(mqParam), mqKey, mqDoing);
                SuccessConsole(opt);
                opt = null;
            } else {
                mqAdd(mqKey);
            }
            resolve();
        })
    } else if (mqParam.hasOwnProperty('socketId')) {
        if (ioSocket[socketId]) {
            let opt = {
                title: 'MQ Task Start',
                pathName: __filename,
                message: mqKey
            }
            ioSocket[socketId].emit(mqName, JSON.stringify(mqParam), mqKey, mqDoing);
            SuccessConsole(opt);
            opt = null;
        } else {
            mqAdd(mqKey);
        }
        resolve();
    }
    mqKeyList = null;
    resolve();
})

/**
 * @description 定时检查pending队列任务
 * @param {*} io
 * @param {*} ioSocket
 */
const mqCheckPend = (io, ioSocket) =>{
    checkPendSchedule = schedule.scheduleJob(CHECKPENDSCHEDULESPE, async () => {
        scheduleMess('CheckPend', 1);
        let loopKey = 0;
        do {
            let mess = await redisServer.actionForClient(client => client.BLPOPAsync(PENDINGKEY, 5));
            let mqKey = mess ? mess[1] : null;
            if (mqKey === null) {
                loopKey = 100;
                checkPend = false;
                scheduleMess('CheckPend', 0);
            }
            else await mqPendResolute(mqKey, io, ioSocket);
            loopKey += 1;
        } while (loopKey < 10);
        mess = null;
        scheduleMess('CheckPend', -1);
    });
}

/**
 * @description 定时检查doing队列任务
 */
const mqCheckDoing = () => {
    checkDoingSchedule = schedule.scheduleJob(CHECKDOINGSCHEDULESPE, async () => {
        scheduleMess('CheckDoing', 1);
        let doingObj = await redisServer.hgetAll(DOINGKEY);
        if (isNotEmpty(doingObj)) {
            let nowTimestamp = new Date().valueOf();
            let timeoutKeyList = Object.keys(doingObj).filter(key => {
                let val = JSON.parse(doingObj[key]);
                return (nowTimestamp - val.timestamp) > 60 * 1000;
            });
            await mqAck(timeoutKeyList);
            await mqAdd(timeoutKeyList);
            nowTimestamp = timeoutKeyList = null;
        } else {
            checkDoingSchedule.cancelNext();
            checkDoing = false;
            scheduleMess('CheckDoing', 0);
            return;
        }
        doingObj = null;
        scheduleMess('CheckDoing', -1);
    });   
}

/**
 * @description 定时触发添加获取今日更新任务
 */
const mqStartNormalHourUpdateTask = () => {
    normalHourSchedule = schedule.scheduleJob(HOURGETUPDATESCHEDULESPE, () => {
        scheduleMess('NormalHourUpdateTask', 1);        
        let param = {
            room: ROOMCRAWLERNAME
        }
        let taskName = createMQTaskName(MQAUTO, CWNORMALHOURUPDATE, param);
        mqAdd(taskName);
        param = taskName = null;
        scheduleMess('NormalHourUpdateTask', -1);
    }); 
};

const mqStartZeroPointUpdateTask = () => {
    zeroPointSchedule = schedule.scheduleJob(ZEROPOINTSCHEDULESPE, () => {
        scheduleMess('ZeroPointUpdateTask', 1);        
        let param = {
            room: ROOMCRAWLERNAME
        }
        let taskName = createMQTaskName(MQAUTO, CWZEROPOINTUPDATE, param);
        mqAdd(taskName);
        param = taskName = null;
        scheduleMess('ZeroPointUpdateTask', -1);
    });
}

module.exports = {
    createMQTaskName,
    mqAdd,
    mqAddFirst,
    mqDoing,
    mqAck,
    mqError,
    mqCheckPend,
    mqCheckDoing,
    mqStartNormalHourUpdateTask,
    mqStartZeroPointUpdateTask
}

// (() => {
//     const {createMQTaskName, mqAdd} = require('./')
//     const {MQAUTO, ROOMCRAWLERNAME} = require('../controler/const');
//     const {CWNORMALHOURUPDATE} = require('../socketio/taskName')
//     let param = {
//         room: ROOMCRAWLERNAME
//     }
//     let taskName = createMQTaskName(MQAUTO, CWNORMALHOURUPDATE, param);
//     mqAdd(taskName);
//     param = taskName = null;
// })()

// mongoServer.actionForClient(client => 
//     client.db('dmgou').collection('comic').find().toArray()
// ).then(list => redisServer.lPush(PENDINGKEY, list.map(obj => {
//     let r = createMQTaskName(MQAUTO, 'crawler-dmzj-by-name-update', {
//         name: obj.n,
//         author: obj.a,
//         _id: obj._id,
//         room: ROOMCRAWLERNAME
//     });
//     return r;
// })))