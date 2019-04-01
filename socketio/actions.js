const { SuccessConsole } = require('dizzyl-util/es/log/ChalkConsole');
const { isNotEmpty } = require('dizzyl-util/es/type');
const { ROOMCRAWLERNAME, ROOMCRUDNAME, ROOMIMAGE } = require('./oldConst');
const { setMQTask, setHash2Doing, updateMQTask,
        addMQTaskToRoom, addSortedSet2Error,
        getMQRoomList, getRoomTask, getHashByKey, getHashDoing,
        delRedisKey, delHashKey2Doing } = require('../dbs/actions');

let opt = {
    title: 'MQ Task Message',
    pathName: __filename,
    message: ''
};

let socketMapper = {};

/**
 * @description 处理socket.io client连接成功
 * @param {*} socket
 * @returns {Number} socketMapper的数量
 */
const handleConnect = socket => {
    opt.message = 'welcome to dizzyl-MQ, ' + socket.id;

    SuccessConsole(opt);

    socketMapper[socket.id] = socket;

    return Object.keys(socketMapper).length;
}
exports.handleConnect = handleConnect;


/**
 * @description 处理socket.io client连接关闭
 * @param {*} socket
 * @returns {Number} socketMapper的数量
 */
const handleDisconnect = socket => {
    delete socketMapper[socket.id];

    opt.message = 'bye bye! ' + socket.id;

    SuccessConsole(opt);

    return Object.keys(socketMapper).length;
}
exports.handleDisconnect = handleDisconnect;


/**
 * @description 处理socket.io client加入房间
 * @param {*} socket
 * @param {*} roomName (ROOMCRAWLERNAME, ROOMCRUDNAME, ROOMIMAGE)
 */
const handleJoinRoom = (socket, roomName) => {
    if (![ROOMCRAWLERNAME, ROOMCRUDNAME, ROOMIMAGE].includes(roomName)) return ;

    socket.join(roomName);

    opt.message = `join ${roomName} room ${socket.id}`;

    SuccessConsole(opt);
}
exports.handleJoinRoom = handleJoinRoom;


/**
 * @description 处理从client端接收到的任务信息
 * @param {*} io
 * @param {*} params { to, socketId, taskName, instant(是否即时), ...other }
 */
const handleGetClientEmitTask = async (io, params) => {
    let { to, socketId, taskName, instant } = params;

    if (!taskName) return;

    if (!to && !socketId) return;

    let p = {
        ...params,
        room: to,
    };

    let redisKey = await setMQTask(p);
    
    if (!!instant) {
        p.redisKey = redisKey;

        handleResolveTaskParam(io, p);
    }

    p = null;
}
exports.handleGetClientEmitTask = handleGetClientEmitTask;


/**
 * @description 获取并处理任务队列
 * @param {*} io
 */
const handleGetTask = async (io) => {
    let redisRoomKeyList = await getMQRoomList();

    if (isNotEmpty(redisRoomKeyList)) {
        let promiseList = redisRoomKeyList.map(roomKey => getRoomTask(roomKey, 10));

        let pResultList = await Promise.all(promiseList).catch(err => []);

        let taskKeyList = Array.from(new Set(pResultList.reduce((total, current) => [...total, ...current], [])));

        promiseList = taskKeyList.map(taskKey => getHashByKey(taskKey)
                                                        .then(obj => ({...obj, redisKey: taskKey})));

        let taskParamList = await Promise.all(promiseList).catch(err => []);

        taskParamList.forEach(param => handleResolveTaskParam(io, param));

        promiseList = pResultList = taskKeyList = taskParamList = null;
    }

    return;
}
exports.handleGetTask = handleGetTask;


/**
 * @description 解析并处理Task Param
 * @param {*} io
 * @param {*} param task的param { room, socketId, taskName, ...otherParams }
 */
const handleResolveTaskParam = async (io, param) => {
    let { room, socketId, taskName, ...otherParams } = param;

    if (!!room) { // 房间广播
        io.to(room).clients(async (error, clients) => {
            if (error) throw error;

            if (clients.length > 0) {
                io.in(room).emit(taskName, otherParams);
            } else {
                if (!!otherParams.redisKey)
                    await addMQTaskToRoom(room, otherParams.redisKey);
            }
        });
    }

    if (!!socketId) {   // socket直传
        if (socketMapper[socketId]) {
            socketMapper[socketId].emit(taskName, otherParams);
        } else {
            if (!!otherParams.redisKey)
                await addMQTaskToRoom(room, otherParams.redisKey);
        }
    }

    if (!room && !socketId) { // 传参错误
        if (!!otherParams.redisKey) {
            await updateMQTask(otherParams.redisKey, {errMsg: '传参错误, 缺少room / socketId'});

            await handleError(otherParams.redisKey);
        }
    }

    return;
}
exports.handleResolveTaskParam = handleResolveTaskParam;


/**
 * @description 处理添加redisKey到doing Hash
 * @param {*} redisKey
 */
const handleAddDoing = async (redisKey) => {
    let setParam = {
        [redisKey]: new Date().valueOf(),
    };

    await setHash2Doing(setParam);

    setParam = null;
}
exports.handleAddDoing = handleAddDoing;

/**
 * @description 处理删除redisKey从doing hash
 * @param {*} redisKey (String, Array)
 */
const handleFinish = async (redisKey) => {
    let delKeyList = Array.isArray(redisKey) ? redisKey : [redisKey];

    await delHashKey2Doing(delKeyList);

    delKeyList = null;
}
exports.handleFinish = handleFinish;


/**
 * @description 处理删除redisKey
 * @param {*} redisKey (String, Array)
 */
const handleDelete = async (redisKey) => {
    let keyList = Array.isArray(redisKey) ? redisKey : [redisKey];

    await handleFinish(keyList);

    await delRedisKey(keyList);

    keyList = null;
}
exports.handleDelete = handleDelete;


/**
 * @description 处理添加redis Error & 删除redisKey
 * @param {*} redisKey (String, Array)
 */
const handleError = async (redisKey) => {
    let todayMess = new Date().toLocaleString().split(' ')[0],
        scorenValList = [new Date(todayMess).valueOf(), todayMess], 
        scoren = new Date().valueOf(),
        keyList = Array.isArray(redisKey) ? redisKey : [redisKey];

    let valList = await Promise.all(keyList.map(key => getHashByKey(key)))
                                .then(obj => JSON.stringify(obj))
                                .catch(err => []);

    valList.forEach(val => {
        scorenValList.push(scoren, val);
    });

    await addSortedSet2Error(scorenValList);

    await handleDelete(keyList);

    scorenValList = scoren = keyList = valList = null;
}
exports.handleError = handleError;

/**
 * @description 处理检查doing hash, 超时的在diong Hash删除并添加到error Set
 * @param {number} [overtimeMin=10] 超时(Min)
 */
const handleCheckDoing = async (overtimeMin = 10) => {
    let doingObj = await getHashDoing();

    let overtimeKeyList = [], currentTimestamp = new Date().valueOf();

    for (let redisKey in doingObj) {
        let timestamp = doingObj[redisKey];

        if ((currentTimestamp - timestamp) > overtimeMin * 60 * 1000) 
            overtimeKeyList.push(redisKey);
    }

    await delHashKey2Doing(overtimeKeyList);

    await handleError(overtimeKeyList);

    doingObj = overtimeKeyList = null;
}
exports.handleCheckDoing = handleCheckDoing;