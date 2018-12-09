const {redisServer} = require('../dbs');
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
} = require( './const'))
const {
    CWNORMALHOURUPDATE, 
    CWZEROPOINTUPDATE,
} = require( '../socketio/taskName'))
let checkPendSchedule = null,
    checkDoingSchedule = null,
    normalHourSchedule = null,
    zeroPointSchedule = null;

/** 
 * 退出取消定时任务 && 回收变量
 */
process.on('exit', () => {
    if (checkPendSchedule) checkPendSchedule.cancel();
    if (checkDoingSchedule) checkDoingSchedule.cancel();
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
    else s = '启动';

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
 * @description 添加任务
 * @param {String|Array} mess socket.id||taskName||JSON.stringify(paramObj)
 * paramObj = {room?, socketId?, ...}
 */
const mqAdd = async (mess) => {
    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.RPUSHAsync(PENDINGKEY, ...mess) : 
            client.RPUSHAsync(PENDINGKEY, mess)
    );
    if (checkPendSchedule) checkPendSchedule.reschedule(CHECKPENDSCHEDULESPE);
    if (checkDoingSchedule) checkDoingSchedule.reschedule(CHECKDOINGSCHEDULESPE);
    scheduleMess('All', 1);
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
    await redisServer.actionForClient(client =>
        Array.isArray(mess) ? client.RPUSHAsync(ERRORKEY, ...mess) : 
            client.RPUSHAsync(ERRORKEY, mess)
    );
}

/**
 * @description 定时检查pending队列任务
 * @param {*} io
 * @param {*} ioSocket
 */
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
    mqDoing,
    mqAck,
    mqError,
    mqCheckPend,
    mqCheckDoing,
    mqStartNormalHourUpdateTask,
    mqStartZeroPointUpdateTask
}