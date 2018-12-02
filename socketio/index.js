const {Redis} = require('dizzyl-util/es/dbs');
const redisServer = new Redis({
    "host": "127.0.0.1",
    "port": 6379
});

const createIOServer = (server) => {
    const io = require('socket.io')(server);
    io.on('connection', (socket) => {
        console.log('A socket.io client connected!');

        socket.on('disconnect', (reason) => {
            let rooms = Object.keys(socket.rooms);
            console.log(`${rooms} disconnected, because ${reason}`);
        });

        socket.on('error', (error) => {
            // ...
        });

        socket.on('ms-crawler', async (url) => {
            await redisServer.actionForClient(client => client.RPUSHAsync('mq-crawler', url));
            socket.emit('crawler', url, (result) => {
                console.log(result); 
            });
        });

        socket.on('ms-crud', (msg) => {
            console.log('message: ' + msg);
        })
    });
}

module.exports = createIOServer;
