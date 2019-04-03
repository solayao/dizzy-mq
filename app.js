const Koa = require('koa');
const Router = require('koa-router');
const app = new Koa();
const router = new Router();
const json = require('koa-json');
const onerror = require('koa-onerror');
const bodyparser = require('koa-bodyparser');
const logger = require('koa-logger');
const cors = require('koa2-cors');
const { io } = require('./socketio');
const { handleGetClientEmitTask, handleGetTask } = require('./socketio/actions.js');

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
    .post('/getTask', async (ctx) => {
        await handleGetTask(io);

        ctx.body = 'Get Task Successful.';
    })
    .post('/setTask', async (ctx, next) => {
        let result = await handleGetClientEmitTask(io, ctx.request.body);

        ctx.body = result ? 'Create Task Successful.' : '参数不正确.';
    });

app.use(router.routes())
    .use(router.allowedMethods());

// error-handling
app.on('error', (err, ctx) => {
    console.error('server error', err, ctx)
});
  
module.exports = app;