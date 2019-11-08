# js-sentenai

> Standalone JavaScript client for Sentenai

[![Build Status](https://travis-ci.org/Sentenai/js-sentenai.svg?branch=master)](https://travis-ci.org/Sentenai/js-sentenai) [![npm version](https://badge.fury.io/js/sentenai.svg)](https://badge.fury.io/js/sentenai)

This library is still unstable. Install an exact version to avoid breaking changes.

```shell
$ npm install --save --save-exact sentenai
```

## usage

```js
const Sentenai = require('sentenai');
const sentenai = new Sentenai.Client({ auth_key: '' });

const pattern = await sentenai.savePattern('my-stream-id when temp > 82.3');
const spans = await pattern.search();
```

View [our docs](http://docs.sentenai.com/) to learn more.

## development

All testing is currently handled by [`jest`](https://facebook.github.io/jest/).

```shell
$ npm install
$ npm test
```

To automatically run `jest` as you update code:

```shell
$ npm test -- --watch
```

To evaluate current testing coverage:

```shell
$ npm test -- --coverage

# generate html report in coverage/
$ npm test -- --coverage --coverageReporters "html"
$ open coverage/index.html
```
