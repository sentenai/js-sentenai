require('es6-shim');
const https = require('https');

// var SentenaiException = function () { };

class QueryResult {
  constructor (client, spans) {
    this.client = client;
    this.spans = spans;
  }

  get stats () {
    var max;
    var min;
    var tot = 0;
    var dts = [];
    for (var i = 0; i < this.spans.length; i++) {
      var dt = this.spans[i].end - this.spans[i].start;
      dts.push(dt);
      tot = tot + dt;
      min = dt >= min ? min : dt;
      max = dt <= max ? max : dt;
    }
    return {
      count: this.spans.length,
      mean: tot / dts.length,
      min: min,
      max: max,
      median: dts.sort()[Math.floor(dts.length / 2)]
    };
  }

  spans (resolve, reject) {
    const ps = [];
    for (var i = 0; i < this.spans.length; i++) {
      var c = this.spans[i].cursor;
      var p = new Promise(function (resolve, reject) {
        var req = this.protocol.request({
          hostname: this.host,
          path: '/query/' + c,
          method: 'GET',
          headers: {
            'auth-key': this.client.auth_key,
            'Content-Type': 'application/json'
          }
        }, function (response) {
          response.on('data', function (chunk) { body.push(chunk); });
          response.on('end', function () {
            const events = JSON.parse(body.join());
            return {s: this.spans[i].start, e: this.spans[i].end, evts: events};
          });
        });
      });
      ps.push(p);
    }
    return new Promise(function (resolve) {
      var segments = sp.evts;
      const slices = [];
      for (var i = 0; i < segments.length; i++) {
        var slice = {};
        for (var k in segments[i]) {
          slice[k] = {stream: segments[i][k].stream, events: []};
        }
        for (var j = 0; j < segments[i].events; j++) {
          var evt = segments[i].events[j];
          slice[evt.stream].events.push(evt.event);
        }
        slices.push({streams: slice, start: sp.s, end: sp.e});
      }
      resolve(slices);
    });
  }
}

class Client {
  constructor (config) {
    this.auth_key = config.auth_key;
    this.protocol = https;
    this.host = 'api.senten.ai';
  }

  query (q) {
    return new Promise(function (resolve, reject) {
      const body = [];
      const req = this.protocol.request({
        hostname: this.host,
        path: '/query',
        method: 'POST',
        headers: {
          'auth-key': this.auth_key,
          'Content-Type': 'application/json'
        }
      }, function (response) {
        response.on('data', function (chunk) { body.push(chunk); });
        response.on('end', function () {
          var rspans = JSON.parse(body.join());
          var spans = [];
          for (var i = 0; i < rspans.length; i++) {
            spans.push({
              'start': new Date(rspans[i].start),
              'end': new Date(rspans[i].end),
              'cursor': rspans[i].cursor
            });
          }
          resolve(new QueryResult(spans));
        });
      });

      req.on('error', function (err) { reject(err); });
      req.write(JSON.stringify(q.ast));
      req.end();
    });
  }
}

module.exports = Client;
