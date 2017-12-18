# js-sentenai

> Standalone JavaScript client for Sentenai

[![Build Status](https://travis-ci.org/Sentenai/js-sentenai.svg?branch=master)](https://travis-ci.org/Sentenai/js-sentenai) [![npm version](https://badge.fury.io/js/sentenai.svg)](https://badge.fury.io/js/sentenai)

```shell
$ npm install --save sentenai
```

## usage

```js
const { Client, flare } = require('sentenai');
const { stream, select, gt } = flare;
const sentenai = new Client({ auth_key: '' });

const myStream = stream('my-stream-id');
const json = await sentenai.query(
  select()(
    myStream({ temp: gt(82.3) })
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
