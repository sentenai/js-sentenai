module.exports = {
  env: {
    development: {
      presets: [
        [
          '@babel/preset-env',
          {
            targets: {
              chrome: 75,
              ie: 11,
              esmodules: true
            }
          }
        ]
      ]
    },
    test: {
      presets: [
        [
          '@babel/preset-env',
          {
            targets: {
              node: 'current'
            }
          }
        ]
      ]
    }
  }
};
