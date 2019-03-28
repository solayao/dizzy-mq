const {
    createMQTaskName,
    mqAck, 
    mqAdd,
    mqError, 
    mqDoing,
    mqPendResolute,
    mqCheckPend, 
    mqCheckDoing, 
    mqStartTodayCheckUpdateTask,
    mqStartTodayLastCheckUpdateTask,
    socketAddRoom
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
    BECRAWLERCH,
    IMGSTARTUPLOAD
} = require('./taskName');
const {SuccessConsole} = require('dizzyl-util/es/log/ChalkConsole');

const io = require('socket.io')();
exports.io = io;
let ioSocket = {};
exports.ioSocket = ioSocket;
let opt = {
    title: 'MQ Task Message',
    pathName: __filename,
    message: ''
};

process.on('exit', () => {
    ioSocket = opt = null;
});

io.on('connection', socket => {
    /* socket.io client连接成功 */   
    opt.message = 'welcome to dizzyl-MQ, ' + socket.id;
    SuccessConsole(opt);
    ioSocket[socket.id] = socket;
    /* socket.io client连接关闭 */
    socket.on('disconnect', function () {
        delete ioSocket[socket.id];
        opt.message = 'bye bye! ' + socket.id;
        SuccessConsole(opt);
    });

    socket.on(MQTASKDOING, mqKey => mqDoing(mqKey));
    socket.on(MQTASKERROR, mqKey => mqError(mqKey));
    socket.on(MQTASKFINISH, mqKey => mqAck(mqKey));

    /* 加入crawler房间 */
    socket.on(JOINCRAWLER, () => {
        socket.join(ROOMCRAWLERNAME);
        socketAddRoom(ROOMCRAWLERNAME);
        opt.message = `join ${ROOMCRAWLERNAME} room ${socket.id}`;
        SuccessConsole(opt);
    });
    /* 加入crud房间 */
    socket.on(JOINCRUD, () => {
        socket.join(ROOMCRUDNAME);
        socketAddRoom(ROOMCRUDNAME);
        opt.message = `join ${ROOMCRUDNAME} room ${socket.id}`;
        SuccessConsole(opt);
    });
    /* 加入Image房间 */
    socket.on(JOINIMAGE, () => {
        socket.join(ROOMIMAGE);
        socketAddRoom(ROOMIMAGE);
        opt.message = `join ${ROOMIMAGE} room ${socket.id}`;
        SuccessConsole(opt);
    });

    /* 通过Chapter的id去查询Chapter的图片列表, 立即触发 */
    socket.on(FECRAWLERCH, async ch => {
        let param = {
            ch,
            socket: socket.id,
            room: ROOMCRAWLERNAME
        };
        let taskName = createMQTaskName(socket.id, CWSTARTCH, param);
        await mqPendResolute(taskName, io, ioSocket).then(() => {
            param = taskName = null;
        }).catch(err => {   // 没有对应的crawler
            console.error(err)
            ioSocket[socket.id].emit(FEBACKCRAWLERCH, []);
        });
    });

    /* 通过Chapter的id队列去查询Chapter的图片列表 */
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

    /* 完成通过Ch去查询Chapter的图片列表 */
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
mqCheckPend(io, ioSocket);
/* 定时触发查询今日更新 */
mqStartTodayCheckUpdateTask();
mqStartTodayLastCheckUpdateTask();