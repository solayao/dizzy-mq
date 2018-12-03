const io = require('socket.io')();
const crawler = io.of('/crawler');
const fbe = io.of('/fbe');
const {mqAck, mqAdd, mqDoing} = require('../controler');
const {PENDINGKEY, DOINGKEY} = require('../controler/const');

let crawlerSocket = {}, fbeSocket = {};

crawler.on('connection', socket => {
    console.log('welcome to room-crawler', socket.id);
    crawlerSocket[socket.id] = socket;

    socket.on('finish', result => {
        console.log(result)
        mqAck(result);
        fbeSocket[result.split('||')[0]].emit('finishMQ', '嘻嘻嘻')
    });

    socket.on('disconnect', function () {
        crawlerSocket[socket.id] = undefined;
    });
});

fbe.on('connection', socket => {
    console.log('welcome to room-fbe', socket.id);
    fbeSocket[socket.id] = socket;

    socket.on('disconnect', function () {
        fbeSocket[socket.id] = undefined;
    });

    socket.on('addMQ', data => {
        mqAdd(`${socket.id}||${data}`, () => {
            // crawler.emit('start');
            crawler.clients((error, clients) => {
                if (error) throw error;
                crawlerSocket[clients[0]].emit('start', PENDINGKEY, mqDoing);
            });
        })
    });
});


module.exports = io;
