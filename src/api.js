require('es6-shim');
const https = require('https');

// https://stackoverflow.com/a/27093173
const minDate = new Date(1, 0, 1, 0, 0, 0);
const maxDate = new Date(9990, 11, 31, 23, 59, 59);

const SentenaiException = function () {};

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
      var p = new Promise((resolve, reject) => {
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

class Cursor {
  // TODO: add `returning` a.k.a. projections
  constructor (client, query, queryId) {
    this._client = client;
    this._query = query;
    this._queryId = queryId;
    this._spans = null;
  }

  async json () {
    await this.spans();
    const data = await Promise.all(this._spans.map(async (span) =>
      this._slice(span.cursor, span.start || minDate, span.end || maxDate)
    ));
    return JSON.stringify(data, null, 2);
  }

  async spans () {
    if (!this._spans) {
      let id = this._queryId;
      let allSpans = [];

      while (id) {
        const body = await this._fetchSpan(id);

        const spans = body.spans.map(s => Object.assign({}, s, {
          start: s.start ? new Date(s.start) : null,
          end: s.end ? new Date(s.end) : null
        }));

        id = body.cursor;
        allSpans = allSpans.concat(spans);
      }

      this._spans = allSpans;
    }

    return this._spans.map(s => ({
      start: s.start,
      end: s.end
    }));
  }

  _fetchSpan (id) {
    return new Promise((resolve, reject) => {
      const req = https.get({
        hostname: this._client.host,
        path: `/query/${id}/spans`,
        headers: this._client.getHeaders()
      }, response => {
        const body = [];
        response.on('data', chunk => {
          body.push(chunk);
        });

        response.on('end', () => {
          resolve(JSON.parse(Buffer.concat(body).toString()));
        });
      });

      req.on('error', err => {
        reject(err);
      });
      req.end();
    });
  }

  async _slice (cursorId, start, end, maxRetries = 3) {
    let cursor = `${cursorId.split('+')[0]}+${start.toISOString()}+${end.toISOString()}`;
    const streams = {};

    while (cursor) {
      const response = await this._fetchEvents(cursor, maxRetries);
      cursor = response.cursor;
      const data = response.results;

      Object.keys(data.streams).forEach(queryHash => {
        if (!streams[queryHash]) {
          streams[queryHash] = {
            streams: data.streams[queryHash],
            events: []
          };
        }
      });

      data.events.forEach(event => {
        const { events } = streams[event.stream];
        delete event.stream;
        events.push(event);
      });
    }

    return { start, end, streams: Object.values(streams) };
  }

  async _fetchEvents (cursor, maxRetries, retries = 0) {
    return new Promise((resolve, reject) => {
      const req = https.get({
        hostname: this._client.host,
        path: `/query/${cursor}/events`,
        headers: this._client.getHeaders()
      }, response => {
        const body = [];
        const { statusCode, headers } = response;
        const ok = statusCode >= 200 && statusCode;

        response.on('data', chunk => {
          body.push(chunk);
        });

        response.on('end', () => {
          const results = JSON.parse(Buffer.concat(body).toString());
          if (ok) {
            resolve({
              results,
              cursor: headers.cursor || null
            });
          } else if (retries < maxRetries) {
            // TODO: confirm that `resolve` is the right move here, confirm that retries work
            resolve(this._fetchEvents(cursor, maxRetries, retries + 1));
          } else {
            reject(new SentenaiException('Failed to get cursor'));
          }
        });
      });

      req.on('error', err => {
        reject(err);
      });
      req.end();
    });
  }
}

class Client {
  constructor (config) {
    this.auth_key = config.auth_key;
    this.protocol = https;
    this.host = 'api.sentenai.com';
  }

  getHeaders () {
    return {
      'auth-key': this.auth_key,
      'Content-Type': 'application/json'
    };
  }

  query (query) {
    return new Promise((resolve, reject) => {
      const req = this.protocol.request({
        hostname: this.host,
        path: '/query',
        method: 'POST',
        headers: this.getHeaders()
      }, response => {
        const queryId = response.headers.location;
        resolve(new Cursor(this, query, queryId));
      });

      req.on('error', err => {
        reject(err);
      });

      req.write(JSON.stringify(query.ast));
      req.end();
    });
  }

  streams () {
    return new Promise((resolve, reject) => {
      const req = https.get({
        hostname: this.host,
        path: '/streams',
        headers: this.getHeaders()
      }, response => {
        const body = [];
        response.on('data', chunk => {
          body.push(chunk);
        });

        response.on('end', () => {
          // TODO: create Stream instances
          resolve(JSON.parse(Buffer.concat(body).toString()));
        });
      });

      req.on('error', err => {
        reject(err);
      });
      req.end();
    });
  }
}

module.exports = Client;
