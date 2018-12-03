const io = require('socket.io')();
const crawler = io.of('/crawler');
const fbe = io.of('/fbe');
const {mqAck, mqAdd, mqDoing} = require('../controler');
const {PENDINGKEY, DOINGKEY} = require('../controler/const');
const {Mongo} = require('dizzyl-util/es/dbs');
const {mongoConfig} = require('../schemas/config.json');
const {ObjectId} = require('mongodb');

let crawlerSocket = {}, fbeSocket = {};
const mongoServer = new Mongo(mongoConfig);

const server2client = {
    // dmzj日漫
    'https://manhua.dmzj.com/': '/overseas/dmzj/link/',
    'https://images.dmzj.com/webpic/': '/overseas/dmzj/img/',
    // dmzj国漫
    'https://www.dmzj.com/info/': '/domestic/dmzj/link/',
    'https://images.dmzj.com/img/webpic/': '/domestic/dmzj/img/',
    // dmzj的Chapter图片链接
    '&https://images.dmzj.com/': '/chapter/dmzj/img/',
};
/**
 * @description 适配替换字符
 * @param {*} str
 * @returns {String}
 */
const match2replace = (str) => {
    const urlKeysList = Object.keys(server2client);
    const matchKey = urlKeysList.find(v => str.includes(v));
    return matchKey
        ? str.replace(matchKey, server2client[matchKey])
        : str;
}

crawler.on('connection', socket => {
    console.log('welcome to room-crawler', socket.id);
    crawlerSocket[socket.id] = socket;

    socket.on('finish', (arg0, arg1) => {
        mqAck(arg0);
        fbeSocket[arg0.split('||')[0]].emit('finishMQ-crawlerCH', arg1.map(iUrl => '/getImg' + match2replace('&' + iUrl)));
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

    socket.on('addMQ-crawlerCH', async ch => {
        const qResult = await mongoServer.actionQuery('chapter', {
            _id: ObjectId(ch)
        });
        const chapter = qResult[0];
        mqAdd(`${socket.id}||${chapter.link}`, () => {
            // crawler.emit('start');
            crawler.clients((error, clients) => {
                if (error) throw error;
                crawlerSocket[clients[0]].emit('start', PENDINGKEY, mqDoing);
            });
        })
    });
});


module.exports = io;
