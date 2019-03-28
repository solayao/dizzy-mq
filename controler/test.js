const {createMQTaskName, mqAdd} = require('./index');
const {redisServer, mongoServer} = require('../dbs');
const {
    PENDINGKEY, 
    DOINGKEY, 
    ERRORKEY, 
    MQKEYJOIN, 
    ROOMCRUDNAME,
    ROOMCRAWLERNAME, 
    MQAUTO, 
    CHECKPENDSCHEDULESPE, 
    CHECKDOINGSCHEDULESPE, 
    TODAYCHECKCHEDULESPE, 
    TODAYLASTCHECKSCHEDULESPE
} = require( './const');

// const {BECOMPARECOMIC} = require( '../socketio/taskName');
// redisServer.lRange('lateComic').then(data => {
//         data.forEach(o => {
//             let taskName = createMQTaskName(MQAUTO, BECOMPARECOMIC, {
//                 detail: JSON.parse(o),
//                 room: ROOMCRUDNAME
//             })
//             mqAdd(taskName);
//             taskName = null;
//         })
//     }).then(() => {
//         console.log('end')
//     })

// const {CWSTARTID} = require( '../socketio/taskName');
// redisServer.lRange('recrawlerId').then(data => {
//         data.forEach(o => {
//             let taskName = createMQTaskName(MQAUTO, CWSTARTID, {
//                 comicId: o,
//                 room: ROOMCRAWLERNAME
//             })
//             mqAdd(taskName);
//             taskName = null;
//         })
//     }).then(() => {
//         console.log('end')
//     })

// const {CWSTARTCH} = require('../socketio/taskName');
// mongoServer.actionQuery('chapter', {
//     il: null
// }, 'dmgou').then(result => {
//     result.forEach(o => {
//         let param = {
//             ch: o._id,
//             room: ROOMCRAWLERNAME
//         };
//         let taskName = createMQTaskName(MQAUTO, CWSTARTCH, param);
//         mqAdd(taskName);
//         param = taskName = null;
//     })
// }).then(() => {
//     console.log('end')
// })


// 添加获取所有漫画详情， 用于初始化
// const {CWSTARTID} = require( '../socketio/taskName');
// mongoServer.actionForClient(client => 
//         client.db('dmgou').collection('comic')
//             .find({
//                 d: null
//             })
//             .project({
//                 '_id': 1,
//             })
//             .toArray()
//     ).then(data => {
//         // data.forEach(o => {
//         //     let taskName = createMQTaskName(MQAUTO, CWSTARTID, {
//         //         comicId: o._id,
//         //         room: ROOMCRAWLERNAME
//         //     })
//         //     mqAdd(taskName);
//         //     taskName = null;
//         // })
//         let o = data[0];
//         let taskName = createMQTaskName(MQAUTO, CWSTARTID, {
//             comicId: o._id,
//             room: ROOMCRAWLERNAME
//         })
//         mqAdd(taskName);
//         taskName = null;
//     }).then(() => {
//         console.log('end')
//     })