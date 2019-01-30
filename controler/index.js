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
    TODAYCHECKCHEDULESPE, 
    TODAYLASTCHECKSCHEDULESPE
} = require( './const');
const {
    CWTODAYUPDATE, 
    CWYESTERDAYUPDATE,
} = require( '../socketio/taskName');
const {SuccessConsole} = require('dizzyl-util/es/log/ChalkConsole');

let checkPendSchedule = null, checkPend = true,
    checkDoingSchedule = null, checkDoing = true,
    normalHourSchedule = null,
    zeroPointSchedule = null;

let pendingKeySet = new Set(); 

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
    checkPendSchedule = checkDoingSchedule = 
    normalHourSchedule = zeroPointSchedule = pendingKeySet = null;
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
exports.createMQTaskName = createMQTaskName;

/**
 * @description 拆分MQ的任务名称
 * @param {*} taskName
 * @returns {Object} {socketId, mqName, mqParam}
 */
const analyzeMQTaskName = (taskName) => {
    let mqKeyList = taskName.split(MQKEYJOIN);
    let socketId = mqKeyList[0], mqName = mqKeyList[1], mqParam = JSON.parse(mqKeyList[2]);
    mqKeyList = null;
    return ({
        socketId,
        mqName,
        mqParam
    });
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
 * @description 加入房间
 */
const socketAddRoom = (room) => {
    pendingKeySet.add(PENDINGKEY + '-' + room);
}
exports.socketAddRoom = socketAddRoom;

/**
 * @description 添加优先任务
 * @param {String|Array} mess socket.id||taskName||JSON.stringify(paramObj)
 * paramObj = {room?, socketId?, ...}
 */
const mqAddFirst = async (mess) => {
    await mqAdd(mess, 'LPUSHAsync');
}
exports.mqAddFirst = mqAddFirst;

/**
 * @description 添加任务
 * @param {String|Array} mess socket.id||taskName||JSON.stringify(paramObj)
 * paramObj = {room?, socketId?, ...}
 */
const mqAdd = async (mess, redisFunc = 'RPUSHAsync') => {
    if (Array.isArray(mess) && !isNotEmpty(mess)) return ;

    if (!Array.isArray(mess)) mess = [mess];

    let messObj = mess.reduce((total, current) => {
        let {mqParam} = analyzeMQTaskName(current);

        let redisKey = PENDINGKEY;

        if (mqParam.hasOwnProperty('room')) redisKey += '-' + mqParam.room;

        mqParam = null;

        if (!total[redisKey]) total[redisKey] = [current];
        else total[redisKey].push(current);

        return total;
    }, {});

    await Promise.all(Object.keys(messObj).map(redisKey => redisServer.actionForClient(client =>
        client[redisFunc](redisKey, ...messObj[redisKey]) 
    )));

    messObj = null;
    
    restartCheck();
}
exports.mqAdd = mqAdd;

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
exports.mqDoing = mqDoing;

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
exports.mqAck = mqAck;

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

    await mqAck(mess);
}
exports.mqError = mqError;

/**
 * @description 处理mq-pending
 * @param {String} mqKey
 * @param {*} io
 * @param {*} ioSocket
 */
const mqPendResolute = (mqKey, io, ioSocket) => new Promise(resolve => {
    let {socketId, mqName, mqParam} = analyzeMQTaskName(mqKey);
    if (mqParam.hasOwnProperty('room')) {
        let opt = {
            title: 'MQ Task Start',
            pathName: __filename,
            message: mqKey
        };
        io.to(mqParam.room).clients((error, clients) => {
            if (error) throw error;
            if (clients.length > 0) {
                io.in(mqParam.room).emit(mqName, JSON.stringify(mqParam), mqKey);
                SuccessConsole(opt);
            } else {
                mqAdd(mqKey);
            }
            opt = null;
            resolve();
        });
    } else if (mqParam.hasOwnProperty('socketId')) {
        if (ioSocket[socketId]) {
            let opt = {
                title: 'MQ Task Start',
                pathName: __filename,
                message: mqKey
            }
            ioSocket[socketId].emit(mqName, JSON.stringify(mqParam), mqKey);
            SuccessConsole(opt);
            opt = null;
        } else {
            mqAdd(mqKey);
        }
        resolve();
    }
    resolve();
})

/**
 * @description 定时检查pending队列任务
 */
const mqCheckPend = (io, ioSocket) =>{
    checkPendSchedule = schedule.scheduleJob(CHECKPENDSCHEDULESPE, async () => {
        scheduleMess('CheckPend', 1);
        let loopKey = 0;
        do {
            let promiseList = Array.from(pendingKeySet).map(key => 
                redisServer.actionForClient(client => client.BLPOPAsync(key, 5))
            );
            let resultList = await Promise.all(promiseList);
            let pendKeys = resultList.filter(result => !!result).map(result => result[1]);
            promiseList = resultList = null;
            if (pendKeys.length === 0) {
                loopKey = 100;
                checkPend = false;
                scheduleMess('CheckPend', 0);
            } else {
                promiseList = pendKeys.map(key => mqPendResolute(key, io, ioSocket));
                await Promise.all(promiseList);
                promiseList = pendKeys = null;
            }
            loopKey += 1;
        } while (loopKey < 10);
        mess = null;
        scheduleMess('CheckPend', -1);
    });
}
exports.mqCheckPend = mqCheckPend;

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
exports.mqCheckDoing = mqCheckDoing;

/**
 * @description 定时触发添加获取今日更新任务, 每小时30分执行
 */
const mqStartTodayCheckUpdateTask = () => {
    normalHourSchedule = schedule.scheduleJob(TODAYCHECKCHEDULESPE, () => {
        scheduleMess('TodayCheckUpdateTask', 1);        
        let param = {
            room: ROOMCRAWLERNAME
        }
        let taskName = createMQTaskName(MQAUTO, CWTODAYUPDATE, param);
        mqAddFirst(taskName);
        param = taskName = null;
        scheduleMess('TodayCheckUpdateTask', -1);
    }); 
};
exports.mqStartTodayCheckUpdateTask = mqStartTodayCheckUpdateTask;

/**
 * @description 定时触发添加获取最后今日的更新任务, 每天23：58：30执行
 */
const mqStartTodayLastCheckUpdateTask = () => {
    zeroPointSchedule = schedule.scheduleJob(TODAYLASTCHECKSCHEDULESPE, () => {
        scheduleMess('TodayLastCheckUpdateTask', 1);        
        let param = {
            room: ROOMCRAWLERNAME
        }
        let taskName = createMQTaskName(MQAUTO, CWYESTERDAYUPDATE, param);
        mqAddFirst(taskName);
        param = taskName = null;
        scheduleMess('TodayLastCheckUpdateTask', -1);
    });
}
exports.mqStartTodayLastCheckUpdateTask = mqStartTodayLastCheckUpdateTask;


// 添加获取所有漫画详情， 用于初始化
// const {CWSTARTID} = require( '../socketio/taskName');
// mongoServer.actionForClient(client => 
//         client.db('dmgou').collection('comic')
//             .find()
//             .project({
//                 '_id': 1,
//             })
//             .toArray()
//     ).then(data => {
//         data.forEach(o => {
//             let taskName = createMQTaskName(MQAUTO, CWSTARTID, {
//                 comicId: o._id,
//                 room: ROOMCRAWLERNAME
//             })
//             mqAdd(taskName);
//             taskName = null;
//         })
//     }).then(() => {
//         console.log('end')
//     })