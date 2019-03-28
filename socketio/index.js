const io = require('socket.io')();
const schedule = require('node-schedule');
const { handleConnect, handleDisconnect, handleJoinRoom, handleGetClientEmitTask, handleGetTask, handleAddDoing } = require('./actions');
const GETTASKSPE = '*/10 * * * * *';

io.on('connection', socket => {
    let scheduleGetTask, currentSocketNum = 0;

    currentSocketNum = handleConnect(socket);

    if (currentSocketNum > 0 && !!scheduleGetTask) {
        scheduleGetTask.reschedule(GETTASKSPE);
    }

    socket.on('disconnect', () => {
        currentSocketNum = handleDisconnect(socket);

        if (currentSocketNum === 0 && !!scheduleGetTask) {
            scheduleGetTask.cancelNext();
        }
    });

    socket.on('join-room', roomName => handleJoinRoom(socket, roomName));

    socket.on('mq-task-doing', redisKey => handleAddDoing(redisKey));
    
    socket.on('mq-task-finish', redisKey => {});

    socket.on('mq-task-error', redisKey => {});

    socket.on('client-emit-task', params => handleGetClientEmitTask(io, params));

    scheduleGetTask = schedule.scheduleJob(GETTASKSPE, () => handleGetTask(io));
})