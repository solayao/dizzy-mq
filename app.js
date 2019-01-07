const Koa = require('koa');
const Router = require('koa-router');
const app = new Koa();
const router = new Router();
const views = require('koa-views');
const json = require('koa-json');
const onerror = require('koa-onerror');
const bodyparser = require('koa-bodyparser');
const logger = require('koa-logger');
const cors = require('koa2-cors');
const {createMQTaskName, mqAddFirst} = require('./controler');
const {MQAUTO} = require('./controler/const');

// error handler
onerror(app);

// middlewares
app.use(bodyparser({
    enableTypes:['json', 'form', 'text']
}));
app.use(json());
app.use(logger());

// logger
app.use(async (ctx, next) => {
    const start = new Date()
    await next()
    const ms = new Date() - start
    console.log(`${ctx.method} ${ctx.url} - ${ms}ms`)
});

// CORS
app.use(cors({
    credentials: true,
    allowMethods: ['GET', 'POST', 'HEADER'],
}));

// routes
router
    .get('/', (ctx, next) => {
        ctx.body = 'Hello World!';
    })
    .get('/setTask', async (ctx, next) => {
        let {taskName, ...taskParams} = ctx.request.query;
        let task = createMQTaskName(MQAUTO, taskName, taskParams);
        await mqAddFirst(task);
        taskName = taskParams = task = null;
        ctx.body = 'Add Task Successful!';
    });

app.use(router.routes())
    .use(router.allowedMethods());

// error-handling
app.on('error', (err, ctx) => {
    console.error('server error', err, ctx)
});
  
module.exports = app;