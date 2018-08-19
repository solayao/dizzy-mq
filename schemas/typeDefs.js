const typeDefs = `
type User{
    id:Int!,
    name:String!
}
type Query {
    users: [User]
}
schema {
    query: Query
}
`;

module.exports = typeDefs;