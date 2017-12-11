require('es6-shim');
const fetch = require('node-fetch');

const { ast, stream } = require('./flare');

// https://stackoverflow.com/a/27093173
const minDate = new Date(1, 0, 1, 0, 0, 0);
const maxDate = new Date(9990, 11, 31, 23, 59, 59);

const SentenaiException = function () {};
const sum = list => list.reduce((total, num) => total + num, 0);

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

  // Get time-based stats about query results in milliseconds
  async stats () {
    await this.spans();

    // TODO: why would a span _not_ have a start / end?
    const deltas = this._spans.filter(
      s => s.start && s.end
    ).map(s => s.end - s.start);

    // Don't divide by zero
    if (!deltas.length) return {};

    return {
      count: this._spans.length,
      mean: sum(deltas) / deltas.length,
      min: Math.min.apply(Math, deltas),
      max: Math.max.apply(Math, deltas),
      median: deltas.sort((a, b) => a - b)[Math.floor(deltas.length / 2)]
    };
  }

  _fetchSpan (id) {
    return this._client.fetch(
      `/query/${id}/spans`
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
      `/query/${cursor}/events`
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
  }

  fetch (url, options = {}) {
    return fetch(`${this.host}${url}`, Object.assign({}, options, {
      headers: options.headers || this.getHeaders()
    }));
  }

  getHeaders () {
    return {
      'auth-key': this.auth_key,
      'Content-Type': 'application/json'
    };
  }

  query (query) {
    return this.fetch('/query', {
      method: 'POST',
      body: ast(query)
    }).then(res =>
      new Cursor(this, query, res.headers.get('location'))
    );
  }

  async streams (name = '', meta = {}) {
    const streamList = await this.fetch('/streams').then(res => res.json());
    name = name.toLowerCase();

    return streamList.filter(s => {
      let match = true;
      if (name) {
        match = s.name.toLowerCase().includes(name);
      }
      Object.keys(meta).forEach(key => {
        match = match && s.meta[key] === meta[key];
      });
      return match;
    // TODO: this is weird because `stream` just returns another function.
    // doesn't print well, doesn't inform user of what's going on
    }).map(s => stream(s));
  }

  fields (stream) {
    return this.fetch(
      `/streams/${stream()}/fields`
    ).then(res => res.json());
  }

  values (stream) {
    return this.fetch(
      `/streams/${stream()}/values`
    ).then(res => res.json());
  }

  newest (stream) {
    return this.fetch(
      `/streams/${stream()}/newest`
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
      `/streams/${stream()}/oldest`
    ).then(async (res) => {
      return {
        event: await res.json(),
        ts: new Date(res.headers.get('Timestamp')),
        id: res.headers.get('Location')
      };
    });
  }

  get (stream, eid) {
    const base = `/streams/${stream()}`;
    const url = eid ? `${base}/events/${eid}` : base;

    return this.fetch(url).then(async (res) => {
      // TODO: handle status codes, communicate errors
      if (eid) {
        return {
          id: res.headers.get('location'),
          ts: res.headers.get('timestamp'),
          event: await res.json()
        };
      } else {
        return res.json();
      }
    });
  }

  // TODO:
  // put (stream, event, { eid, timestamp }) {}
  // stats (stream, field, { start, end }) {}

  delete (stream, eid) {
    const url = `/streams/${stream()}/events/${eid}`;
    return this.fetch(url, {
      method: 'delete'
    });
  }

  destroy (stream) {
    const url = `/streams/${stream()}`;
    return this.fetch(url, {
      method: 'delete',
      headers: {
        'auth-key': this.auth_key
      }
    });
  }
}

module.exports = Client;
