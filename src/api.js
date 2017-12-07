require('es6-shim');
const fetch = require('node-fetch');

const { ast } = require('./flare');

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
    return this._client.fetch(
      `${this._client.host}/query/${id}/spans`
    ).then(res => res.json());
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
    return this._client.fetch(
      `${this._client.host}/query/${cursor}/events`
    ).then(async (res) => {
      if (res.ok) {
        const results = await res.json();

        return {
          results,
          cursor: res.headers.get('cursor') || null
        };
      } else if (retries < maxRetries) {
        return this._fetchEvents(cursor, maxRetries, retries + 1);
      } else {
        throw new SentenaiException('Failed to get cursor');
      }
    });
  }
}

class Client {
  constructor (config) {
    this.auth_key = config.auth_key;
    this.host = 'https://api.sentenai.com';
    this.fetch = (url, options) => {
      return fetch(url, Object.assign({}, options, {
        headers: this.getHeaders()
      }));
    };
  }

  getHeaders () {
    return {
      'auth-key': this.auth_key,
      'Content-Type': 'application/json'
    };
  }

  query (query) {
    return this.fetch(`${this.host}/query`, {
      method: 'POST',
      body: ast(query)
    }).then(res =>
      new Cursor(this, query, res.headers.get('location'))
    );
  }

  streams () {
    return this.fetch(`${this.host}/streams`).then(res => res.json());
  }

  fields (stream) {
    return this.fetch(
      `${this.host}/streams/${stream()}/fields`
    ).then(res => res.json());
  }

  values (stream) {
    return this.fetch(
      `${this.host}/streams/${stream()}/values`
    ).then(res => res.json());
  }

  newest (stream) {
    return this.fetch(
      `${this.host}/streams/${stream()}/newest`
    ).then(async (res) => {
      return {
        event: await res.json(),
        ts: new Date(res.headers.get('Timestamp')),
        id: res.headers.get('Location')
      };
    });
  }

  oldest (stream) {
    return this.fetch(
      `${this.host}/streams/${stream()}/oldest`
    ).then(async (res) => {
      return {
        event: await res.json(),
        ts: new Date(res.headers.get('Timestamp')),
        id: res.headers.get('Location')
      };
    });
  }
}

module.exports = Client;
