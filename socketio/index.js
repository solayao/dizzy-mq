const io = require('socket.io')();

const {mqAck, mqAdd, mqCheckPend} = require('../controler');
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

    socket.on('join-Crawlerroom', () => {
        socket.join('crawler');
        console.log('join crawler room', socket.id);
    });

    socket.on('addMQ-crawlerCH', ch => {
        mqAdd(`${socket.id}||start-crawler-ch||${ch}`)
    });

    socket.on('finish-crawler-ch', (mqKey, imgList) => {
        let returnSocket = ioSocket[mqKey.split('||')[0]];
        if (returnSocket) returnSocket.emit('finishMQ-crawlerCH', imgList);
        else console.log('socket连接不存在');
        mqAck(mqKey);
    });
});

mqCheckPend(io, ioSocket);

module.exports = io;
