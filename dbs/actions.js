const shortid = require('shortid');
const { getPrototypeType } = require('dizzyl-util/es/type');
const { redisServer } = require('./index');

const redisKeyForRoomList = 'MQ-ROOM-LIST';
const redisKeyForRoomTaskBefore = 'MQ-TASK-';

/**
 * @description 通过params设置MQ Task到redis
 * @param {Object} params 带设置的参数对象
 * @returns {String} redisKey
 */
const setMQTask = async (params) => {
    let {room = 'null'} = params,
        redisKey = 'mq-' + room + '-' + shortid.generate();
    
    await redisServer.hmSet(redisKey, params);

    let redisRoomKey = await addMQTaskToRoom(room, redisKey);

    await redisServer.actionForClient(client => client.SADDAsync(redisKeyForRoomList, redisRoomKey));

    return redisKey;
}
exports.setMQTask = setMQTask;


/**
 * @description 将MQ Task Key添加到对应的Room List
 * @param {*} room
 * @param {*} redisKey
 * @returns {String} redisRoomKey
 */
const addMQTaskToRoom = async (room, redisKey) => {
    if (!room) return ;

    let keyList = [];

    switch (getPrototypeType(redisKey)) {
        case 'String':  keyList.push(redisKey); break;
        case 'Array':   keyList = redisKey; break;
        default: {}
    }

    let redisRoomKey = redisKeyForRoomTaskBefore + room.toUpperCase();

    if (keyList.length)
        await redisServer.actionForClient(client => client.RPUSHAsync(redisRoomKey, ...keyList))

    keyList = null;

    return redisRoomKey;
}
exports.addMQTaskToRoom = addMQTaskToRoom;


/**
 * @description 获取MQ的Room Set
 * @returns {Array}
 */
const getMQRoomList = () => 
    redisServer.actionForClient(client => client.SMEMBERSAsync(redisKeyForRoomList))
exports.getMQRoomList = getMQRoomList;


/**
 * @description 获取对应Room的任务key队列
 * @param {String} roomKey 
 * @param {Number} taskNum = 1
 * @return {Array} [ redisKey ]
 */
const getRoomTask = async (roomKey, taskNum = 1) => {
    let i = 1, resultList = [];

    do {
        let taskKey = await redisServer.actionForClient(client => client.BLPOPAsync(roomKey, 5));

        if (!!taskKey) 
            resultList.push(taskKey[1])

        i++;
    } while (i <= taskNum)

    return resultList;
}
exports.getRoomTask = getRoomTask;

/**
 * @description 通过key获取hash的所有参数
 * @param {*} hashKey
 */
const getHashByKey = (hashKey) => 
    redisServer.hgetAll(hashKey)
exports.getHashByKey = getHashByKey;


/**
 * @description 设置doing的redisKey
 * @param {Object} hash
 */
const setHash2Doing = (hash) =>
    redisServer.hmSet('', hash)
exports.setHash2Doing = setHash2Doing;