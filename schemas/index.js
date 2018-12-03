// const { makeExecutableSchema } = require('graphql-tools');
const typeDefs = require('./typeDefs');
const resolvers = require('./resolvers');
const {ApolloServer} = require('apollo-server-koa');
const {Redis, Mongo} = require('dizzyl-util/es/dbs');
const {redisConfig, mongoConfig} = require('./config.json');

const redisServer = new Redis(redisConfig);
const mongoServer = new Mongo(mongoConfig);

module.exports = new ApolloServer({
    typeDefs,
    resolvers,
    context: ({req}) => ({redisServer, mongoServer})
});
