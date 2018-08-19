// const { makeExecutableSchema } = require('graphql-tools');
const typeDefs = require('./typeDefs');
const resolvers = require('./resolvers');
const { ApolloServer } = require('apollo-server-koa');

module.exports = new ApolloServer({
    typeDefs,
    resolvers,
});
