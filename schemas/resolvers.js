// const dbOperation = require('../dbs/MongoOperation');

const resolvers = {
  Query: {
    users(root, args, context) {
      return [{ id: 1, name: 'wenshao' }];
    },
  }
};

module.exports = resolvers;