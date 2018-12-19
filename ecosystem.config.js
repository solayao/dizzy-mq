module.exports = {
  apps : [{
    name      : 'DizzyMQ',
    script    : 'bin/www.js',
    env: {
      NODE_ENV: 'production',
      PORT: '4627'
    },
  }],
};
