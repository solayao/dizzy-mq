const io = require('socket.io')();
const {
    createMQTaskName,
    mqAck, 
    mqAdd, 
    mqAddFirst,
    mqError, 
    mqDoing,
    mqCheckPend, 
    mqPendResolute,
    mqCheckDoing, 
    mqStartNormalHourUpdateTask,
    mqStartZeroPointUpdateTask
} = require('../controler');
const {
    MQAUTO,
    MQKEYJOIN, 
    ROOMCRAWLERNAME, 
    ROOMCRUDNAME,
    ROOMIMAGE
} = require('../controler/const');
const {  
    JOINCRAWLER,
    JOINCRUD,
    JOINIMAGE,
    MQTASKDOING,
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
    BECOMPARECOMIC,
    BESTARTIMGUPLOAD,
    IMGSTARTUPLOAD
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

    socket.on(MQTASKDOING, mqKey => mqDoing(mqKey));
    socket.on(MQTASKERROR, mqKey => mqError(mqKey));
    socket.on(MQTASKFINISH, mqKey => mqAck(mqKey));

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
    /* 加入Image房间 */
    socket.on(JOINIMAGE, () => {
        socket.join(ROOMIMAGE);
        console.log(`join ${ROOMIMAGE} room ${socket.id}`);
    });

    /* 通过Chapter的id去查询comic内容, 立即触发 */
    socket.on(FECRAWLERCH, ch => {
        let param = {
            ch,
            socket: socket.id,
            room: ROOMCRAWLERNAME
        };
        let taskName = createMQTaskName(socket.id, CWSTARTCH, param);
        mqPendResolute(taskName).then(() => {
            param = taskName = null;
        });
    });

    /* 通过Chapter的id队列去查询comic内容 */
    socket.on(BECRAWLERCH, chList => {
        chList.forEach(ch => {
            let param = {
                ch,
                room: ROOMCRAWLERNAME
            };
            let taskName = createMQTaskName(MQAUTO, CWSTARTCH, param);
            mqAdd(taskName);
            param = taskName = null;
        });
    });

    /* 完成通过Ch去查询comic内容 */
    socket.on(CWFINISHCH, (mqKey, imgList) => {
        mqAck(mqKey);
        let mqKeyValList = mqKey.split(MQKEYJOIN);
        if (mqKeyValList[0] !== MQAUTO) { // 回应 FECRAWLERCH
            let returnSocket = ioSocket[mqKeyValList[0]];
            if (returnSocket)
                returnSocket.emit(FEBACKCRAWLERCH, imgList.map(url => '/getImg/' + url.replace('^', 's')));
            else 
                console.log('socket连接不存在');
        } else { // 回应 BECRAWLERCH
        
        }
       
        if (imgList.length > 0) {
            let param = {
                room: ROOMCRUDNAME,
                ch: JSON.parse(mqKeyValList[2]).ch,
                imgList,
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

    /* 启动漫画图片上传任务*/
    socket.on(BESTARTIMGUPLOAD, () => {
        let param = {
            room: ROOMIMAGE,
            time: new Date().valueOf(),
        };
        let taskName = createMQTaskName(MQAUTO, IMGSTARTUPLOAD, param);
        mqAdd(taskName);
        param = taskName = null;
    });
});
/* 定时检查doing列表 */
mqCheckDoing();
/* 定时检查pending列表 */
mqCheckPend();
/* 定时触发查询今日更新 */
mqStartNormalHourUpdateTask();
mqStartZeroPointUpdateTask();

module.exports = {
    io,
    ioSocket
};
