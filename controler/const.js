const PENDINGKEY = 'mq-pending';
const DOINGKEY = 'mq-doing';
const ERRORKEY = 'mq-error';
const MQKEYJOIN = '||';
const CHECKPENDSCHEDULESPE = '*/10 * * * * *';
const CHECKDOINGSCHEDULESPE = '0 * * * * *';
const HOURGETUPDATESCHEDULESPE = '0 0 1-23 * * *';
const ROOMCRAWLERNAME = 'crawler';
const ROOMCRUDNAME = 'crud';
const MQAUTO = 'MQAUTO';

module.exports = {
    PENDINGKEY,
    DOINGKEY,
    ERRORKEY,
    MQKEYJOIN,
    CHECKPENDSCHEDULESPE,
    CHECKDOINGSCHEDULESPE,
    HOURGETUPDATESCHEDULESPE,
    ROOMCRAWLERNAME,
    ROOMCRUDNAME,
    MQAUTO
}