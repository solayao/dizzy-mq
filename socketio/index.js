const io = require('socket.io')();
const schedule = require('node-schedule');
const { GETTASKSPE, CHECKDOINGSPE } = require('./const');
const { handleConnect, handleDisconnect, handleJoinRoom, handleGetClientEmitTask, handleCheckDoing,
        handleGetTask, handleAddDoing, handleFinish, handleDelete, handleError } = require('./actions');

let scheduleGetTask = schedule.scheduleJob(GETTASKSPE, () => handleGetTask(io)),
    scheduleCheckDoing = schedule.scheduleJob(CHECKDOINGSPE, () => handleCheckDoing());

io.on('connection', socket => {
    let currentSocketNum = 0;

    currentSocketNum = handleConnect(socket);

    if (currentSocketNum > 0) {
        if (!!scheduleGetTask) scheduleGetTask.reschedule(GETTASKSPE);

        if (!!scheduleCheckDoing) scheduleCheckDoing.reschedule(CHECKDOINGSPE);
    }

    socket.on('disconnect', () => {
        currentSocketNum = handleDisconnect(socket);

        if (currentSocketNum === 0) {
            if (!!scheduleGetTask) scheduleGetTask.cancelNext();

            if (!!scheduleCheckDoing) scheduleCheckDoing.cancelNext();
        }
    });

    socket.on('join-room', roomName => handleJoinRoom(socket, roomName));

    socket.on('mq-task-doing', redisKey => handleAddDoing(redisKey));
    
    socket.on('mq-task-finish', redisKey => handleFinish(redisKey));

    socket.on('mq-task-delete', redisKey => handleDelete(redisKey));

    socket.on('mq-task-error', redisKey => handleError(redisKey));

    socket.on('client-emit-task', params => handleGetClientEmitTask(io, params));
})

exports.io = io;