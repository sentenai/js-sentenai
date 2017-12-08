# js-sentenai

> Standalone JavaScript client for Sentenai

```shell
$ npm install --save sentenai
```

## usage

```js
const { Client, flare } = require('sentenai');
const sentenai = new Client({ auth_key: '' });

const myStream = flare.stream('my-stream-id');
const json = await sentenai.query(
  flare.select()(
    myStream({ temp: flare.gt(82.3) })
  )
).then(data => data.json());
```

View [our docs](http://docs.sentenai.com/) to learn more.

## development

All testing is currently handled by [`jest`](https://facebook.github.io/jest/).

```
$ npm install
$ npm test
```
