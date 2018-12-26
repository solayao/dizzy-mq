const io = require('socket.io')();
const {
    createMQTaskName,
    mqAck, 
    mqAdd, 
    mqAddFirst,
    mqError, 
    mqDoing,
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
    MQTASKFINISH,
    MQTASKERROR,
    FECRAWLERCH,
    FEBACKCRAWLERCH,
    FECRAWLERBYID,
    CWSTARTCH,
    CWSTARTID,
    CWFINISHCH,
    CWCOMPARECOMIC,
    BEUPDATECHAPTER,
    BECOMPARECOMIC
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
    socket.on(MQTASKERROR, mqKey => {
        mqError(mqKey);
    });

    socket.on(MQTASKFINISH, mqKey => {
        mqAck(mqKey);
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

    /* 通过Ch去查询comic内容, 立即触发 */
    socket.on(FECRAWLERCH, ch => {
        let param = {
            ch,
            room: ROOMCRAWLERNAME
        };
        let taskName = createMQTaskName(socket.id, CWSTARTCH, param);
        // mqAddFirst(taskName);
        io.to(ROOMCRAWLERNAME).clients((error, clients) => {
            if (error) throw error;
            if (clients[0]) {
                ioSocket[clients[0]].emit(CWSTARTCH, JSON.stringify(param), taskName, mqDoing);
            } else {
                mqAddFirst(taskName);
            }
            param = taskName = null;
        })
    });

    /* 完成通过Ch去查询comic内容 */
    socket.on(CWFINISHCH, (mqKey, imgList) => {
        let mqKeyValList = mqKey.split(MQKEYJOIN);
        let returnSocket = ioSocket[mqKeyValList[0]];
        if (returnSocket) returnSocket.emit(FEBACKCRAWLERCH, imgList);
        else console.log('socket连接不存在');
        mqAck(mqKey);
        if (imgList.length > 0) {
            let param = {
                room: ROOMCRUDNAME,
                ch: JSON.parse(mqKeyValList[2]).ch,
                imgList: imgList
            };
            let taskName = createMQTaskName(MQAUTO, BEUPDATECHAPTER, param);
            mqAdd(taskName);
            param = taskName = null;
        }
        mqKeyValList = null;
    });

    /* 添加对比最新comic任务*/
    socket.on(CWCOMPARECOMIC, latestComicDetailObj => {
        let param = {
            room: ROOMCRUDNAME,
            detail: latestComicDetailObj
        };
        let taskName = createMQTaskName(MQAUTO, BECOMPARECOMIC, param);
        mqAdd(taskName);
        param = taskName = null;
    });

    /* 通过漫画id添加查询最新任务*/
    socket.on(FECRAWLERBYID, id => {
        let param = {
            comicId:id,
            room: ROOMCRAWLERNAME
        };
        let taskName = createMQTaskName(socket.id, CWSTARTID, param);
        mqAdd(taskName);
        param = taskName = null;
    });
});
/* 定时检查pending列表 */
mqCheckPend(io, ioSocket);
/* 定时检查doing列表 */
mqCheckDoing();
/* 定时触发查询今日更新 */
// mqStartNormalHourUpdateTask();
// mqStartZeroPointUpdateTask();

module.exports = io;
