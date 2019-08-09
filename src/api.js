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
        // TODO: ignoring `streams` for now
        return events.map(e =>
          Object.assign({}, e, {
            ts: new Date(e.ts)
          })
        );
      });
  }
}

export class Pattern {
  constructor(client, { name, description, query, anonymous, created }) {
    this._client = client;
    this.name = name;
    this.description = description;
    this.query = query;
    this.anonymous = anonymous;
    this.created = new Date(created);
  }

  spans() {
    return this._client
      .fetch(`/patterns/${this.name}/search`)
      .then(getJSON)
      .then(spans =>
        spans.map(({ start, end }) => ({
          start: new Date(start),
          end: new Date(end)
        }))
      );
  }

  saveAs(name, description = '') {
    return this._client.pattern(this.query, name, description);
  }

  delete() {
    return this._client
      .fetch(`/patterns/${this.name}`, { method: 'DELETE' })
      .then(handleStatusCode);
  }
}

class Client {
  constructor(config) {
    this.auth_key = config.auth_key;
    this.host =
      typeof config.host === 'string'
        ? trimTrailing('/', config.host)
        : 'https://api.sentenai.com';

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

  ping() {
    return this.fetch('/').then(handleStatusCode);
  }

  view(view, name = '', description = '') {
    const url = name ? `/views/${encodeURIComponent(name)}` : '/views';
    const body = name ? { description, view } : { view };
    return this.fetch(url, {
      method: 'POST',
      body: JSON.stringify(body)
    }).then(res => {
      handleStatusCode(res);
      const viewId = res.headers.get('Location');
      return new View(this, viewId, name, description);
    });
  }

  pattern(pattern, name = '', description = '') {
    const url = name ? `/patterns/${encodeURIComponent(name)}` : '/patterns';
    return this.fetch(url, {
      body: JSON.stringify({
        pattern: typeof pattern === 'string' ? pattern : ast(pattern),
        description: name ? description : undefined
      }),
      method: 'POST'
    }).then(res => {
      if (res.status === 201) {
        return new Pattern(this, {
          name: name || res.headers.get('Location'),
          description,
          query: pattern,
          anonymous: !name,
          created: res.headers.get('Date')
        });
      } else if (res.status === 400) {
        return res.json().then(body => {
          throw new SentenaiException(body.message);
        });
      } else {
        return getJSON(res);
      }
    });
  }
  patterns() {
    return this.fetch('/patterns')
      .then(getJSON)
      .then(list =>
        list.map(
          ({ name, description, query, anonymous, created }) =>
            new Pattern(this, {
              name,
              description,
              query,
              anonymous,
              created
            })
        )
      );
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

  values(stream, at) {
    const params = {};
    if (at instanceof Date) {
      params.at = at.toISOString();
    }
    if (stream.filter) {
      params.filters = base64(stream.filter.ast);
    }
    const query = Object.keys(params).length ? '?' + queryString(params) : '';
    return this.fetch(`/streams/${stream.name}${query}`)
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
    const base = `/streams/${stream.name}/stats/${field}`;

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

  // TODO: turn this into `events(start, end, limit, sort, offset, filters)`
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

  toString() {
    // TODO: include filters?
    return this.name;
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

  // TODO: get, put

  stats(field, opts) {
    return this._client.stats(this, field, opts);
  }

  // TODO: calling stats is a bug
  // TODO: turn into `events`, see Client#events
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

export class Field {
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

  stats(opts) {
    return this.stream.stats(this.pathString(), opts);
  }

  pathString() {
    return this.path.join('.');
  }

  toString() {
    return `${this.stream.toString()}:${this.pathString()}`;
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

function trimTrailing(char, str) {
  while (str[str.length - 1] === char) {
    str = str.slice(0, str.length - 1);
  }
  return str;
}

export default Client;
