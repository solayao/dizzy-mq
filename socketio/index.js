const io = require('socket.io')();

const {mqAck, mqAdd, mqError, mqCheckPend, mqCheckDoing, mqGetHourUpdateTask} = require('../controler');
const {MQKEYJOIN, ROOMCRAWLERNAME, ROOMCRUDNAME} = require('../controler/const');
const {  
    JOINCRAWLER,
    JOINCRUD,
    CRAWLERBYCH,
    BACKCRAWLERBYCH,
    STARTCRAWLERBYCH,
    FINISHCRAWLERBYCH,
    ERRORCRAWLERBYCH,
    FINISHHOURUPDATE,
    STARTUPDATETODAYCOMIC
} = require('../controler/taskName');
let ioSocket = {};

process.on('exit', () => {
    ioSocket = null;
});

io.on('connection', socket => {
    console.log('welcome to dizzyl-MQ', socket.id);
    ioSocket[socket.id] = socket;

    socket.on('disconnect', function () {
        delete ioSocket[socket.id];
        console.log('bye bye!', socket.id);
    });

    socket.on(JOINCRAWLER, () => {
        socket.join(ROOMCRAWLERNAME);
        console.log(`join ${ROOMCRAWLERNAME} room ${socket.id}`);
    });

    socket.on(JOINCRUD, () => {
        socket.join(ROOMCRUDNAME);
        console.log(`join ${ROOMCRUDNAME} room ${socket.id}`);
    });

    socket.on(CRAWLERBYCH, ch => {
        let param = {
            ch,
            room: ROOMCRAWLERNAME
        };
        let taskName = socket.id+MQKEYJOIN+STARTCRAWLERBYCH+MQKEYJOIN+JSON.stringify(param);
        mqAdd(taskName);
        param = taskName = null;
    });

    socket.on(FINISHCRAWLERBYCH, (mqKey, imgList) => {
        let returnSocket = ioSocket[mqKey.split(MQKEYJOIN)[0]];
        if (returnSocket) returnSocket.emit(BACKCRAWLERBYCH, imgList);
        else console.log('socket连接不存在');
        mqAck(mqKey);
    });

    socket.on(ERRORCRAWLERBYCH, mqKey => {
        mqError(mqKey);
    });

    socket.on(FINISHHOURUPDATE, mqKey => {
        mqAck(mqKey);
        let param = {
            room: ROOMCRUDNAME
        };
        let taskName = MQAUTO+MQKEYJOIN+STARTUPDATETODAYCOMIC+MQKEYJOIN+JSON.stringify(param);
        mqAdd(taskName);
        param = taskName = null;
    });
});

mqCheckPend(io, ioSocket);
mqCheckDoing();
mqGetHourUpdateTask();

module.exports = io;
