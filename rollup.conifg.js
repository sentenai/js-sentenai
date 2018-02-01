import babel from 'rollup-plugin-babel'
import async from 'rollup-plugin-async'

export default {
  input: 'index.js',
  output: {
    file: 'build/index.js',
    format: 'cjs'
  },
  external: ['isomorphic-fetch'],
  plugins: [
    async(),
    babel()
  ]
}
