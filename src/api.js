import {
  ast,
  FlareException,
  Switch,
  BoundSwitch,
  makeSpans,
  Filter
} from './flare';
import btoa from 'btoa';

// https://stackoverflow.com/a/27093173
const minDate = new Date(1, 0, 1, 0, 0, 0);
const maxDate = new Date(9990, 11, 31, 23, 59, 59);

class SentenaiException extends Error {}
class AuthenticationError extends Error {}
class APIError extends Error {}
class NotFound extends Error {}
const sum = list => list.reduce((total, num) => total + num, 0);

class Query {
  constructor(client, query, queryId, limit) {
    this._client = client;
    this._query = query;
    this._queryId = queryId;
    this._spans = null;
    this._limit = limit;
  }

  getBody() {
    return typeof this._query === 'string'
      ? this._query
      : JSON.stringify(this._query);
  }

  json() {
    return this.spans()
      .then(spans =>
        Promise.all(
          this._spans.map(span =>
            this._slice(span.cursor, span.start || minDate, span.end || maxDate)
          )
        )
      )
      .then(data => JSON.stringify(data, null, 2));
  }

  spans() {
    if (!this._spans) {
      return this._recursiveFetchSpan(this._queryId);
    }
    return Promise.resolve(this._spans);
  }

  _fetchSpan(id, limit) {
    const base = `/query/${id}/spans`;
    const url =
      typeof limit === 'number' ? `${base}?${queryString({ limit })}` : base;
    return this._client.fetch(url).then(getJSON);
  }

  _recursiveFetchSpan(id, allSpans = []) {
    return this._fetchSpan(id, this._limit).then(body => {
      const spans = body.spans
        .map(s =>
          Object.assign({}, s, {
            start: s.start ? new Date(s.start) : null,
            end: s.end ? new Date(s.end) : null
          })
        )
        .map(span => new Span(this._client, span));

      const nextId = body.cursor;
      allSpans = allSpans.concat(spans);

      if (
        !nextId ||
        (typeof this._limit === 'number' && allSpans.length >= this._limit)
      ) {
        this._spans = allSpans;
        return this._spans;
      }

      return this._recursiveFetchSpan(nextId, allSpans);
    });
  }

  // Get time-based stats about query results in milliseconds
  stats() {
    return this.spans().then(() => {
      const deltas = this._spans
        .filter(s => s.start && s.end)
        .map(s => s.end - s.start);

      // Don't divide by zero
      if (!deltas.length) return {};

      return {
        count: this._spans.length,
        mean: sum(deltas) / deltas.length,
        min: Math.min.apply(Math, deltas),
        max: Math.max.apply(Math, deltas),
        median: deltas.sort((a, b) => a - b)[Math.floor(deltas.length / 2)]
      };
    });
  }

  _slice(cursorId, start, end, maxRetries = 3) {
    let cursor = `${
      cursorId.split('+')[0]
    }+${start.toISOString()}+${end.toISOString()}`;
    return this._recursiveSlice(cursor, start, end, maxRetries);
  }

  _recursiveSlice(cursor, start, end, maxRetries, streams = {}) {
    return this._fetchEvents(cursor, maxRetries).then(response => {
      const nextCursor = response.cursor;
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

      if (nextCursor) {
        return this._recursiveSlice(
          nextCursor,
          start,
          end,
          maxRetries,
          streams
        );
      } else {
        return { start, end, streams: Object.values(streams) };
      }
    });
  }

  _fetchEvents(cursor, maxRetries, retries = 0) {
    return this._client.fetch(`/query/${cursor}/events`).then(res => {
      if (res.ok) {
        return getJSON(res).then(results => {
          return {
            results,
            cursor: res.headers.get('cursor') || null
          };
        });
      } else if (retries < maxRetries) {
        return this._fetchEvents(cursor, maxRetries, retries + 1);
      } else {
        throw new SentenaiException('Failed to get cursor');
      }
    });
  }
}

class Span {
  constructor(client, { cursor, start, end }) {
    this._client = client;
    this.cursor = cursor;
    this.start = start;
    this.end = end;
  }

  // TODO: this probably needs to chase `nextCursor`
  events(projections, maxRetries = 3, retries = 0) {
    const cursor = this.cursor;
    const params = {};

    if (projections) {
      params.projections = base64(projections);
    }

    return this._client
      .fetch(`/query/${cursor}/events?${queryString(params)}`)
      .then(res => {
        if (res.ok) {
          return getJSON(res).then(results => {
            return {
              results,
              cursor: res.headers.get('cursor') || null
            };
          });
        } else if (retries < maxRetries) {
          return this.events(projections, maxRetries, retries + 1);
        } else {
          throw new SentenaiException('Failed to get cursor');
        }
      });
  }
}

class View {
  constructor(client, id, name, description) {
    this._client = client;
    this.id = id;
    this.name = name;
    this.description = description;
  }

  data(opts = {}) {
    const { start, end, limit, sort } = opts;
    const params = {};
    if (start) {
      params.start = start.toISOString();
    }
    if (end) {
      params.end = end.toISOString();
    }
    if (limit) {
      params.limit = limit;
    }
    if (sort) {
      params.sort = sort;
    }
    return this._client
      .fetch(`/views/${this.id}/data?${queryString(params)}`)
      .then(getJSON)
      .then(({ streams, events }) => {
        // ignoring `streams` for now
        return events.map(e =>
          Object.assign({}, e, {
            ts: new Date(e.ts)
          })
        );
      });
  }
}

class Client {
  constructor(config) {
    this.auth_key = config.auth_key;
    this.host = config.host || 'https://api.sentenai.com';

    this._fetch =
      typeof window === 'object' && typeof window.fetch === 'function'
        ? window.fetch.bind(window)
        : require('isomorphic-fetch');
  }

  fetch(url, options = {}) {
    return this._fetch(
      `${this.host}${url}`,
      Object.assign({}, options, {
        headers: Object.assign({}, this.getHeaders(), options.headers)
      })
    );
  }

  getHeaders() {
    return {
      'auth-key': this.auth_key,
      'Content-Type': 'application/json'
    };
  }

  query(query, limit) {
    let options =
      typeof query === 'string'
        ? {
            body: query,
            headers: { 'Content-Type': 'text/neoflare' }
          }
        : {
            body: ast(query)
          };

    return this.fetch(
      '/query',
      Object.assign(
        {
          method: 'POST'
        },
        options
      )
    ).then(res => {
      if (res.status === 201) {
        return new Query(this, query, res.headers.get('location'), limit);
      } else if (res.status === 400) {
        return res.json().then(body => {
          throw new SentenaiException(body.message);
        });
      } else {
        return getJSON(res);
      }
    });
  }

  view(view, name = '', description = '') {
    const url = name ? `/views/${encodeURIComponent(name)}` : '/views';
    // TODO: remove `description: ''` once API removes it
    const body = name ? { description, view } : { view, description: '' };
    return this.fetch(url, {
      method: 'POST',
      body: JSON.stringify(body)
    }).then(res => {
      handleStatusCode(res);
      const viewId = res.headers.get('Location');
      return new View(this, viewId, name, description);
    });
  }

  streams(name = '', meta = {}) {
    return this.fetch('/streams')
      .then(getJSON)
      .then(streamList => {
        name = name.toLowerCase();

        return streamList
          .filter(s => {
            let match = true;
            if (name) {
              match = s.name.toLowerCase().includes(name);
            }
            Object.keys(meta).forEach(key => {
              match = match && s.meta[key] === meta[key];
            });
            return match;
          })
          .map(s => new Stream(this, s.name));
      });
  }

  fields(stream) {
    const query = stream.filter ? '?filters=' + base64(stream.filter.ast) : '';
    return this.fetch(`/streams/${stream.name}/fields${query}`)
      .then(getJSON)
      .then(fields => {
        return fields.map(f => ({
          id: f.id,
          path: f.path,
          start: new Date(f.start)
        }));
      });
  }

  values(stream) {
    const query = stream.filter ? '?filters=' + base64(stream.filter.ast) : '';
    return this.fetch(`/streams/${stream.name}/values${query}`)
      .then(getJSON)
      .then(values => {
        return values.map(v => ({
          id: v.id,
          path: v.path,
          value: v.value,
          ts: new Date(v.ts)
        }));
      });
  }

  newest(stream) {
    return this.fetch(`/streams/${stream.name}/newest`).then(res =>
      getJSON(res).then(event => {
        return {
          event,
          ts: new Date(res.headers.get('Timestamp')),
          id: res.headers.get('Location')
        };
      })
    );
  }

  oldest(stream) {
    return this.fetch(`/streams/${stream.name}/oldest`).then(res =>
      getJSON(res).then(event => {
        return {
          event,
          ts: new Date(res.headers.get('Timestamp')),
          id: res.headers.get('Location')
        };
      })
    );
  }

  get(stream, eid) {
    const base = `/streams/${stream.name}`;
    const url = eid ? `${base}/events/${eid}` : base;

    return this.fetch(url).then(res => {
      if (eid) {
        return getJSON(res).then(event => {
          return {
            event,
            id: res.headers.get('location'),
            ts: res.headers.get('timestamp')
          };
        });
      } else {
        return getJSON(res);
      }
    });
  }

  put(stream, event, opts = {}) {
    const { id, timestamp } = opts;

    const headers = this.getHeaders();
    const base = `/streams/${stream.name}/events`;
    const url = id ? `${base}/${id}` : base;

    if (timestamp) {
      headers.timestamp = timestamp.toISOString();
    }

    if (id) {
      return this.fetch(url, {
        headers,
        method: 'put',
        body: JSON.stringify(event)
      }).then(res => {
        handleStatusCode(res);
        return id;
      });
    } else {
      return this.fetch(url, {
        headers,
        method: 'post',
        body: JSON.stringify(event)
      }).then(res => {
        handleStatusCode(res);
        return res.headers.get('location');
      });
    }
  }

  stats(stream, field, opts = {}) {
    const { start, end } = opts;
    const base = `/streams/${stream.name}/fields/${field}/stats`;

    const params = {};
    if (start) {
      params.start = start.toISOString();
    }
    if (end) {
      params.end = end.toISOString();
    }
    if (stream.filter) {
      params.filters = base64(stream.filter.ast);
    }

    const url = Object.keys(params).length
      ? `${base}?${queryString(params)}`
      : base;
    return this.fetch(url).then(getJSON);
  }

  delete(stream, eid) {
    const url = `/streams/${stream.name}/events/${eid}`;
    return this.fetch(url, {
      method: 'delete'
    }).then(handleStatusCode);
  }

  destroy(stream) {
    const url = `/streams/${stream.name}`;
    return this.fetch(url, {
      method: 'delete',
      headers: {
        'auth-key': this.auth_key
      }
    }).then(handleStatusCode);
  }

  range(stream, start, end) {
    const esc = encodeURIComponent;
    const url = `/streams/${stream.name}/start/${esc(
      start.toISOString()
    )}/end/${esc(end.toISOString())}`;
    return this.fetch(url)
      .then(res => {
        handleStatusCode(res);
        return res.text();
      })
      .then(text =>
        JSON.parse(text).map(item =>
          Object.assign({}, item, {
            ts: new Date(item.ts)
          })
        )
      );
  }

  stream(name, filters) {
    return new Stream(this, name, filters);
  }
}

export class Stream {
  constructor(client, name, filter) {
    this.name = name;
    this._client = client;
    this.filter =
      filter && filter.constructor === Object ? new Filter(filter) : filter;
  }

  withFilter(filter) {
    return new Stream(this._client, this.name, filter);
  }

  when(moment) {
    if (moment instanceof Switch) {
      return new BoundSwitch(
        new Stream(this._client, this.name, this.filter),
        moment
      );
    } else {
      return makeSpans(
        new Stream(this._client, this.name, this.filter),
        moment,
        ['event']
      );
    }
  }

  fields() {
    return this._client
      .fields(this)
      .then(fields => fields.map(f => new Field(this, f)));
  }

  values() {
    return this._client.values(this);
  }

  newest() {
    return this._client.newest(this);
  }

  oldest() {
    return this._client.oldest(this);
  }

  // TODO: get, put

  stats(field, opts) {
    return this._client.stats(this, field, opts);
  }

  range(start, end) {
    return this._client.stats(this, start, end);
  }

  get ast() {
    const ast = { name: this.name };
    if (this.filter) {
      ast.filter = this.filter.ast;
    }
    return ast;
  }
}

class Field {
  constructor(stream, { id, path, start }) {
    this.stream = stream;
    this.id = id;
    this.path = path;
    this.start = start;
  }

  withFilter(filter) {
    const stream = this.stream.withFilter(filter);
    return stream
      .fields()
      .then(
        fields =>
          fields.filter(field => field.toString() === this.toString())[0]
      );
  }

  stats() {
    return this.stream.stats(this.toString());
  }

  toString() {
    // TODO: corona#659
    return this.path.join('.');
  }
}

function base64(obj) {
  return btoa(JSON.stringify(obj));
}

function queryString(params) {
  return Object.keys(params)
    .map(k => encodeURIComponent(k) + '=' + encodeURIComponent(params[k]))
    .join('&');
}

function handleStatusCode(res) {
  const code = res.status;

  if (code === 401) {
    throw new AuthenticationError('Invalid API key');
  } else if (code >= 500) {
    throw new SentenaiException('Something went wrong');
  } else if (code === 400) {
    throw new FlareException();
  } else if (code === 404) {
    throw new NotFound();
  } else if (code >= 400) {
    throw new APIError(res);
  }
}

function getJSON(res) {
  handleStatusCode(res);
  return res.json();
}

export default Client;
