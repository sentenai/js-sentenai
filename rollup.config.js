import babel from 'rollup-plugin-babel';

export default {
  input: 'index.js',
  output: {
    file: 'build/index.js',
    format: 'umd',
    name: 'Sentenai',
    extend: true
  },
  external: ['isomorphic-fetch'],
  plugins: [babel()]
};
