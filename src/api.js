import {
  ast,
  FlareException,
  Switch,
  BoundSwitch,
  makeSpans,
  Filter
} from './flare';
import btoa from 'btoa';

export default class Client {
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
    }).then((res) => {
      handleStatusCode(res);
      return new View(this, {
        name: name || res.headers.get('Location'),
        description,
        anonymous: !name,
        created: res.headers.get('Date'),
        view
      });
    });
  }
  views(opts = {}) {
    // name, desc, containing, limit
    return this.fetch(`/views?${queryString(opts)}`)
      .then(getJSON)
      .then((list) =>
        list.map(
          ({ name, description, streams, view, anonymous, created }) =>
            new View(this, {
              name,
              description,
              view,
              anonymous,
              created
            })
        )
      );
  }

  getPattern(name_) {
    return this.fetch(`/patterns/${name_}`).then((res) => {
      return getJSON(res).then(
        ({ name, streams, query, description, created }) =>
          new Pattern(this, {
            name: name || name_,
            description,
            query,
            created: created || res.headers.get('Date'),
            anonymous: !created
          })
      );
    });
  }

  savePattern(pattern, name = '', description = '') {
    const url = name ? `/patterns/${encodeURIComponent(name)}` : '/patterns';
    return this.fetch(url, {
      body: JSON.stringify({
        pattern: pattern.ast || pattern,
        description: name ? description : undefined
      }),
      method: 'POST'
    }).then((res) => {
      if (res.status === 201) {
        return new Pattern(this, {
          name: name || res.headers.get('Location'),
          description,
          query: pattern,
          anonymous: !name,
          created: res.headers.get('Date')
        });
      } else if (res.status === 400) {
        return res.json().then((body) => {
          throw new SentenaiException(body.message);
        });
      } else {
        return getJSON(res);
      }
    });
  }

  patterns(opts = {}) {
    return this.fetch(`/patterns?${queryString(opts)}`)
      .then(getJSON)
      .then((list) =>
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

  streams(opts = {}) {
    const params = {};
    const { q, name, limit, skip } = opts;
    if (q) {
      params.q = base64(q);
    }
    if (name) {
      params.name = name;
    }
    if (typeof limit === 'number') {
      params.limit = limit;
    }
    if (typeof skip === 'number') {
      params.skip = skip;
    }
    const query = Object.keys(params).length ? '?' + queryString(params) : '';
    return this.fetch(`/streams${query}`)
      .then(getJSON)
      .then((streamList) =>
        streamList.map((s) => new Stream(this, s.name, s.meta))
      );
  }

  fields(stream) {
    const query = stream.filter ? '?filters=' + base64(stream.filter.ast) : '';
    return this.fetch(`/streams/${stream.name}/fields${query}`)
      .then(getJSON)
      .then((fields) => {
        return fields.map((f) => ({
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
      .then((values) => {
        return values.map((v) => ({
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

    return this.fetch(url).then((res) => {
      if (eid) {
        return getJSON(res).then((event) => {
          return {
            event,
            id: res.headers.get('location'),
            ts: new Date(res.headers.get('timestamp'))
          };
        });
      } else {
        return getJSON(res);
      }
    });
  }

  upload(stream, event, opts = {}) {
    const { id, timestamp } = opts;

    const base = `/streams/${stream.name}/events`;
    const url = id ? `${base}/${id}` : base;

    const headers = {};
    if (timestamp) {
      headers.timestamp =
        timestamp instanceof Date ? timestamp.toISOString() : timestamp;
    }

    if (id) {
      return this.fetch(url, {
        headers,
        method: 'PUT',
        body: JSON.stringify(event)
      }).then((res) => {
        handleStatusCode(res);
        return id;
      });
    } else {
      return this.fetch(url, {
        headers,
        method: 'POST',
        body: JSON.stringify(event)
      }).then((res) => {
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

  uniques(stream, field, opts = {}) {
    const { start, end } = opts;
    const base = `/streams/${stream.name}/uniques/${field}`;

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

  events(stream, opts = {}) {
    const { start, end, limit, sort, offset } = opts;
    const params = {};
    if (start) {
      params.start = start.toISOString();
    }
    if (end) {
      params.end = end.toISOString();
    }
    if (typeof limit === 'number') {
      params.limit = limit;
    }
    if (typeof offset === 'number') {
      params.offset = offset;
    }
    if (sort) {
      params.sort = sort;
    }
    if (stream.filter) {
      params.filters = base64(stream.filter.ast);
    }

    const url = `/streams/${stream.name}/events?${queryString(params)}`;
    return this.fetch(url)
      .then(getJSON)
      .then((events) =>
        events.map((e) => ({
          ...e,
          ts: new Date(e.ts)
        }))
      );
  }

  stream(name, filters) {
    return new Stream(this, name, null, filters);
  }
}

export class Stream {
  constructor(client, name, meta, filter) {
    this.name = name;
    this.meta = meta;
    this._client = client;
    this.filter =
      filter && filter.constructor === Object ? new Filter(filter) : filter;
  }

  toString() {
    // TODO: include filters?
    return this.name;
  }

  create(t0) {
    const headers = {};
    if (typeof t0 === 'number') {
      headers.t0 = t0;
    } else if (t0 instanceof Date) {
      headers.t0 = t0.getTime();
    }
    return this._client
      .fetch(`/streams/${this.name}`, {
        headers,
        method: 'POST'
      })
      .then((res) => {
        handleStatusCode(res);
        return this;
      });
  }

  ensureExistence(t0) {
    const headers = {};
    if (typeof t0 === 'number') {
      headers.t0 = t0;
    } else if (t0 instanceof Date) {
      headers.t0 = t0.getTime();
    }
    return this._client
      .fetch(`/streams/${this.name}`, {
        headers,
        method: 'PUT'
      })
      .then((res) => {
        handleStatusCode(res);
        return this;
      });
  }

  getMeta() {
    return this._client.fetch(`/streams/${this.name}/metadata`).then(getJSON);
  }

  setMeta(meta) {
    return this._client
      .fetch(`/streams/${this.name}/metadata`, {
        method: 'PUT',
        body: JSON.stringify(meta)
      })
      .then((res) => {
        handleStatusCode(res);
        return new Stream(this._client, this.name, meta, this.filter);
      });
  }

  patchMeta(meta) {
    return this._client
      .fetch(`/streams/${this.name}/metadata`, {
        method: 'PATCH',
        body: JSON.stringify(meta)
      })
      .then((res) => {
        handleStatusCode(res);
        return new Stream(this._client, this.name, meta, this.filter);
      });
  }

  getMetaKey(key) {
    return this._client
      .fetch(`/streams/${this.name}/metadata/${key}`)
      .then(getJSON);
  }

  setMetaKey(key, value) {
    return this._client
      .fetch(`/streams/${this.name}/metadata/${key}`, {
        method: 'PUT',
        body: JSON.stringify(value)
      })
      .then((res) => {
        handleStatusCode(res);
      });
  }

  upload(event, opts = {}) {
    return this._client.upload(this, event, opts);
  }

  withFilter(filter) {
    return new Stream(this._client, this.name, this.meta, filter);
  }

  when(moment) {
    if (moment instanceof Switch) {
      return new BoundSwitch(
        new Stream(this._client, this.name, this.meta, this.filter),
        moment
      );
    } else {
      return makeSpans(
        new Stream(this._client, this.name, this.meta, this.filter),
        moment,
        ['event']
      );
    }
  }

  fields() {
    return this._client
      .fields(this)
      .then((fields) => fields.map((f) => new Field(this, f)));
  }

  values() {
    return this._client.values(this);
  }

  // TODO: put
  get(eid) {
    return this._client.get(this, eid);
  }

  delete() {
    return this._client
      .fetch(`/streams/${this.name}`, { method: 'DELETE' })
      .then(handleStatusCode);
  }

  stats(field, opts) {
    return this._client.stats(this, field, opts);
  }

  uniques(field, opts) {
    return this._client.uniques(this, field, opts);
  }

  events(opts = {}) {
    return this._client.events(this, opts);
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
      .then((fields) =>
        fields.find((field) => field.pathString() === this.pathString())
      );
  }

  stats(opts) {
    return this.stream.stats(this.pathString(), opts);
  }

  uniques(opts) {
    return this.stream.uniques(this.pathString(), opts);
  }

  pathString() {
    return this.path.join('.');
  }

  toString() {
    return `${this.stream.toString()}:${this.pathString()}`;
  }
}

export class Pattern {
  constructor(client, { name, description, query, anonymous, created }) {
    this._client = client;
    this.name = name;
    this.description = description || null;
    this.anonymous = anonymous || false;
    this.created = new Date(created);
    this.query = query;
  }

  search(opts = {}) {
    const params = {};
    if (opts.start) {
      params.start = opts.start.toISOString();
    }
    if (opts.end) {
      params.end = opts.end.toISOString();
    }
    if (typeof opts.limit === 'number') {
      params.limit = opts.limit;
    }
    if (typeof opts.timeout === 'number') {
      params.timeout = opts.timeout;
    }
    return this._client
      .fetch(`/patterns/${this.name}/search?${queryString(params)}`)
      .then(getJSON)
      .then((spans) =>
        spans.map(({ start, end }) => ({
          start: new Date(start),
          end: new Date(end)
        }))
      );
  }

  saveAs(name, description = '') {
    return this._client.savePattern(this.query, name, description);
  }

  delete() {
    return this._client
      .fetch(`/patterns/${this.name}`, { method: 'DELETE' })
      .then(handleStatusCode);
  }
}

export class View {
  constructor(client, { name, description, anonymous, created, view }) {
    this._client = client;
    this.name = name;
    this.description = description || '';
    this.anonymous = anonymous || false;
    this.created = new Date(created);
    this.view = view;
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
      .fetch(`/views/${this.name}/data?${queryString(params)}`)
      .then(getJSON)
      .then(({ streams, events }) => {
        // TODO: ignoring `streams` for now
        return events.map((e) =>
          Object.assign({}, e, {
            ts: new Date(e.ts)
          })
        );
      });
  }
}

function base64(obj) {
  return btoa(JSON.stringify(obj));
}

function queryString(params) {
  return Object.keys(params)
    .map((k) => encodeURIComponent(k) + '=' + encodeURIComponent(params[k]))
    .join('&');
}

function handleStatusCode(res) {
  const code = res.status;

  if (code === 401 || code === 403) {
    throw new AuthenticationError('Invalid API key');
  } else if (code >= 500) {
    throw new SentenaiException('Something went wrong');
  } else if (code === 400) {
    // TODO: pass response body `message` thru
    throw new BadRequest();
  } else if (code === 404) {
    throw new NotFound();
  } else if (code >= 400) {
    throw new APIError(`${res.status} ${res.statusText}`);
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

// https://stackoverflow.com/a/27093173
const minDate = new Date(1, 0, 1, 0, 0, 0);
const maxDate = new Date(9990, 11, 31, 23, 59, 59);

class SentenaiException extends Error {
  constructor(message) {
    super(message);
    this.name = 'SentenaiException';
  }
}
class AuthenticationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AuthenticationError';
  }
}
class APIError extends Error {
  constructor(message) {
    super(message);
    this.name = 'APIError';
  }
}
class BadRequest extends Error {
  constructor(message) {
    super(message);
    this.name = 'BadRequest';
  }
}
class NotFound extends Error {
  constructor(message) {
    super(message);
    this.name = 'NotFound';
  }
}

export const errors = {
  SentenaiException,
  AuthenticationError,
  APIError,
  NotFound
};
