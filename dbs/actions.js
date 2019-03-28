const shortid = require('shortid');
const {redisServer} = require('./index');

/**
 * @description 通过params设置MQ Task到redis
 * @param {Object} params 带设置的参数对象
 */
const setMQTask = async (params) => {
    let {room = 'null'} = params,
        redisKey = 'mq-' + room + '-' + shortid.generate(),
        redisRoomKey = 'MQ-' + room;
    
    await redisServer.hmSet(redisKey, params);

    await redisServer.lPush(redisRoomKey, [redisKey]);

    await redisServer.actionForClient(client => client.SADDAsync('MQROOM', redisRoomKey));
}
exports.setMQTask = setMQTask;


/**
 * @description 获取MQ的Room Set
 * @returns {Array}
 */
const getMQRoomList = () => 
    redisServer.actionForClient(client => client.SMEMBERSAsync('MQROOM'))
exports.getMQRoomList = getMQRoomList;