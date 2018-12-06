const io = require('socket.io')();
const {
    mqAck, 
    mqAdd, 
    mqError, 
    mqCheckPend, 
    mqCheckDoing, 
    mqStartNormalHourUpdateTask,
    mqStartZeroPointUpdateTask
} = require('../controler');
const {
    MQAUTO,
    MQKEYJOIN, 
    ROOMCRAWLERNAME, 
    ROOMCRUDNAME
} = require('../controler/const');
const {  
    JOINCRAWLER,
    JOINCRUD,
    CRAWLERBYCH,
    BACKCRAWLERBYCH,
    STARTCRAWLERBYCH,
    FINISHCRAWLERBYCH,
    ERRORCRAWLERBYCH,
    FINISHHOURUPDATE,
    STARTUPDATETODAYCOMIC,
    FINISHUPDATETODAYCOMIC,
    STARTUPDATECHAPTER
} = require('./taskName');
let ioSocket = {};

process.on('exit', () => {
    ioSocket = null;
});

io.on('connection', socket => {
    /* socket.io client连接成功 */    
    console.log('welcome to dizzyl-MQ', socket.id);
    ioSocket[socket.id] = socket;
    /* socket.io client连接关闭 */
    socket.on('disconnect', function () {
        delete ioSocket[socket.id];
        console.log('bye bye!', socket.id);
    });
    /* 加入crawler房间 */
    socket.on(JOINCRAWLER, () => {
        socket.join(ROOMCRAWLERNAME);
        console.log(`join ${ROOMCRAWLERNAME} room ${socket.id}`);
    });
    /* 加入crud房间 */
    socket.on(JOINCRUD, () => {
        socket.join(ROOMCRUDNAME);
        console.log(`join ${ROOMCRUDNAME} room ${socket.id}`);
    });
    /* 通过Ch去查询comic内容 */
    socket.on(CRAWLERBYCH, ch => {
        let param = {
            ch,
            room: ROOMCRAWLERNAME
        };
        let taskName = socket.id+MQKEYJOIN+STARTCRAWLERBYCH+MQKEYJOIN+JSON.stringify(param);
        mqAdd(taskName);
        param = taskName = null;
    });
    /* 完成通过Ch去查询comic内容 */
    socket.on(FINISHCRAWLERBYCH, (mqKey, imgList) => {
        let mqKeyValList = mqKey.split(MQKEYJOIN);
        let returnSocket = ioSocket[mqKeyValList[0]];
        if (returnSocket) returnSocket.emit(BACKCRAWLERBYCH, imgList);
        else console.log('socket连接不存在');
        mqAck(mqKey);
        if (imgList.length > 0) {
            let param = {
                room: ROOMCRUDNAME,
                ch: JSON.parse(mqKeyValList[2]).ch,
                imgList: imgList
            };
            let taskName = MQAUTO+MQKEYJOIN+STARTUPDATECHAPTER+MQKEYJOIN+JSON.stringify(param);
            mqAdd(taskName);
            param = taskName = null;
        }
        mqKeyValList = null;
    });
    /* 通过Ch去查询comic内容报错 */
    socket.on(ERRORCRAWLERBYCH, mqKey => {
        mqError(mqKey);
    });
    /* 完成定时触发查询今日更新 */
    socket.on(FINISHHOURUPDATE, mqKey => {
        mqAck(mqKey);
        let param = {
            room: ROOMCRUDNAME
        };
        let taskName = MQAUTO+MQKEYJOIN+STARTUPDATETODAYCOMIC+MQKEYJOIN+JSON.stringify(param);
        mqAdd(taskName);
        param = taskName = null;
    });
    /* 完成把redis的今日更新漫画更新到mongo */
    socket.on(FINISHUPDATETODAYCOMIC, mqKey => {
        mqAck(mqKey);
    });
});
/* 定时检查pending列表 */
mqCheckPend(io, ioSocket);
/* 定时检查doing列表 */
mqCheckDoing();
/* 定时触发查询今日更新 */
mqStartNormalHourUpdateTask();
mqStartZeroPointUpdateTask();

module.exports = io;
