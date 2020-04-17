const fetchMock = require('fetch-mock');
const { Stream, Field, Pattern, View, errors } = require('../src/api.js');
const Client = require('../src/api.js').default;

function mockClient(matcher, response, opts) {
  let client = new Client({
    host: ''
  });
  client._fetch = fetchMock.sandbox().mock(matcher, response, opts);
  return client;
}

/*
  ~~ Client ~~
*/

test('create client with trailing /', () => {
  let host = 'https://my.sentenai.com';
  let client = new Client({
    host: host + '/'
  });

  expect(client.host).toEqual(host);
});

test('Client#ping', () => {
  return mockClient('/', 200)
    .ping()
    .then(res => {
      expect(res).toBe(undefined);
    });
});

test('Client#streams', () => {
  let client = mockClient('/streams', [
    {
      meta: {
        city: 'Boston'
      },
      name: 'weather'
    }
  ]);
  return client.streams().then(streams => {
    expect(streams).toHaveLength(1);
    streams.forEach(stream => {
      expect(stream).toBeInstanceOf(Stream);
      expect(stream.meta.city).toEqual('Boston');
    });
  });
});

test('Client#streams 403', () => {
  let client = mockClient('/streams', new Response({}, { status: 403 }));
  return client.streams().catch(err => {
    expect(err).toBeInstanceOf(errors.AuthenticationError);
  });
});

test('Client#streams 404', () => {
  let client = mockClient('/streams', new Response({}, { status: 404 }));
  return client.streams().catch(err => {
    expect(err).toBeInstanceOf(errors.NotFound);
  });
});

test('Client#patterns', () => {
  let client = mockClient('/patterns', [
    {
      created: '2019-08-08T18:14:19.131384517Z',
      description: '',
      name: 'high-humidity',
      query: "weather when ('humidity' > 0.8)",
      streams: [
        {
          name: 'weather'
        }
      ]
    }
  ]);
  return client.patterns().then(patterns => {
    expect(patterns).toHaveLength(1);
    let first = patterns[0];
    expect(first).toBeInstanceOf(Pattern);
    expect(first.anonymous).toEqual(false);
    expect(first.name).toEqual('high-humidity');
    expect(first.created).toBeInstanceOf(Date);
  });
});

test('Client#pattern neoflare', () => {
  let query = 'weather when humidity > 0.7';
  let loc = 'abc';
  let client = mockClient(
    '/patterns',
    new Response(
      { pattern: query },
      { status: 201, headers: { Location: loc } }
    )
  );

  return client.savePattern(query).then(pattern => {
    expect(pattern.query).toEqual(query);
    expect(pattern.name).toEqual(loc);
    expect(pattern.anonymous).toEqual(true);
    expect(pattern.created).toBeInstanceOf(Date);
  });
});

test('Client#pattern named neoflare', () => {
  let query = 'weather when temp < 32.0';
  let name = 'colddd';
  let client = mockClient(
    `/patterns/${name}`,
    new Response({ pattern: query }, { status: 201 })
  );

  return client.savePattern(query, name).then(pattern => {
    expect(pattern.query).toEqual(query);
    expect(pattern.name).toEqual(name);
    expect(pattern.anonymous).toEqual(false);
    expect(pattern.created).toBeInstanceOf(Date);
  });
});

test('Client#pattern anonymous flare-core', () => {
  let loc = 'abc';
  let query = {
    select: {
      path: ['event', 'humidity'],
      op: '>',
      arg: { type: 'double', val: 0.5 },
      stream: { name: 'weather' },
      type: 'span'
    }
  };
  let client = mockClient(
    `/patterns`,
    new Response(
      { pattern: query },
      { status: 201, headers: { Location: loc } }
    )
  );

  return client.savePattern(query).then(pattern => {
    expect(pattern.query).toEqual(query);
    expect(pattern.name).toEqual(loc);
    expect(pattern.anonymous).toEqual(true);
    expect(pattern.created).toBeInstanceOf(Date);
  });
});

test('Client#views', () => {
  let client = mockClient(`/views`, [
    {
      created: '2019-08-28T17:36:37.951819011Z',
      description: 'hi',
      name: 'some-dew',
      streams: [
        {
          filter: {
            expr: true
          },
          name: 'weather'
        }
      ],
      view: {
        projection: {
          'weather:dewPoint': [
            {
              var: ['event', 'dewPoint']
            }
          ]
        },
        stream: {
          filter: {
            expr: true
          },
          name: 'weather'
        }
      }
    }
  ]);

  return client.views().then(views => {
    expect(views).toHaveLength(1);
    views.forEach(view => {
      expect(view).toBeInstanceOf(View);
      expect(view.anonymous).toEqual(false);
      expect(view.name).toEqual('some-dew');
      expect(view.created).toBeInstanceOf(Date);
    });
  });
});

test('Client#views 403', () => {
  let client = mockClient('/views', new Response({}, { status: 403 }));
  return client.views().catch(err => {
    expect(err).toBeInstanceOf(errors.AuthenticationError);
  });
});

test('Client#view', () => {
  let viewAst = {
    description: 'hi',
    view: {
      stream: { name: 'weather', filter: { expr: true } },
      projection: {
        '...': false,
        'weather:dewPoint': [{ var: ['event', 'dewPoint'] }]
      }
    }
  };
  let loc = 'abc123';
  let client = mockClient(
    '/views',
    new Response('', { status: 201, headers: { Location: loc } })
  );

  return client.view(viewAst).then(view => {
    expect(view.name).toEqual(loc);
    expect(view.description).toEqual('');
    expect(view.anonymous).toEqual(true);
    expect(view.created).toBeInstanceOf(Date);
    expect(view.view).toMatchObject(viewAst);
  });
});

test('Client#get stream 404', () => {
  let name = 'hello';
  let client = mockClient(
    `/streams/${name}`,
    new Response({ code: 404 }, { status: 404 })
  );

  let stream = client.stream(name);
  stream.create().catch(err => {
    expect(err).toBeInstanceOf(errors.NotFound);
  });
});

/*
  ~~ Stream ~~
*/

test('Stream#fields', () => {
  let name = 'weather';
  let client = mockClient(`/streams/${name}/fields`, [
    {
      id: 'GpWlKdKZoWBQd6WOthPHaJCt',
      path: ['humidity'],
      start: '2010-01-01T00:00:00Z'
    },
    {
      id: 'GpWlKdKZoWBQd6WOthPHaJCt',
      path: ['moonPhase'],
      start: '2010-01-01T00:00:00Z'
    }
  ]);
  let stream = client.stream(name);
  return stream.fields().then(fields => {
    expect(fields).toHaveLength(2);
    fields.forEach(f => {
      expect(f).toBeInstanceOf(Field);
      expect(f.start).toBeInstanceOf(Date);
    });
  });
});

test('Stream#values', () => {
  let name = 'big-data';
  let client = mockClient(`/streams/${name}`, [
    {
      id: 'XTlo-cry0yMB8PLUB0u2CbhX',
      path: ['summary'],
      ts: '2010-12-11T00:00:00Z',
      value: 'Mostly cloudy in the morning.'
    },
    {
      id: 'XTlo-cry0yMB8PLUB0u2CbhX',
      path: ['humidity'],
      ts: '2010-12-11T00:00:00Z',
      value: 0.67
    }
  ]);
  let stream = client.stream(name);
  return stream.values().then(values => {
    expect(values).toHaveLength(2);
    values.forEach(value => {
      expect(value.value).toBeDefined();
      expect(value.id).toBeDefined();
      expect(value.ts).toBeInstanceOf(Date);
      expect(value.path).toBeInstanceOf(Array);
    });
  });
});

test('Stream#events', () => {
  let name = 'big-data';
  let client = mockClient(`/streams/${name}/events?limit=2`, [
    {
      duration: null,
      event: {},
      id: 'XTlo-cry0yMB8PLUB0u2CbhX',
      ts: '2010-12-11T00:00:00Z'
    },
    {
      duration: null,
      event: {},
      id: 'XTlo-cry0yMB8PLUB0u2CbhX',
      ts: '2010-12-11T00:00:00Z'
    }
  ]);

  let stream = client.stream(name);
  return stream.events({ limit: 2 }).then(events => {
    expect(events).toHaveLength(2);
    events.forEach(({ duration, event, ts, id }) => {
      expect(duration).toBeNull();
      expect(event).toMatchObject({});
      expect(ts).toBeInstanceOf(Date);
      expect(typeof id).toBe('string');
    });
  });
});

test('Stream#upload', () => {
  let name = 'big-data';
  let loc = 'abc';
  let client = mockClient(
    `/streams/${name}/events`,
    new Response('', { status: 201, headers: { Location: loc } })
  );
  let stream = client.stream(name);
  return stream.upload({ arbitrary: 'event' }).then(id => {
    expect(id).toEqual(loc);
  });
});

test('Stream#get', () => {
  let name = 'big-data';
  let client = mockClient(`/streams/${name}`, [
    {
      id: 'abc123',
      path: ['some', 'field'],
      ts: '2020-03-15T19:49:52.156781851Z',
      value: 'value'
    }
  ]);
  let stream = client.stream(name);
  return stream.get().then(fields => {
    expect(fields).toEqual([
      {
        id: 'abc123',
        path: ['some', 'field'],
        ts: '2020-03-15T19:49:52.156781851Z',
        value: 'value'
      }
    ]);
  });
});

test('Stream#get an event', () => {
  let name = 'big-data';
  let id = 'abc123';
  let data = {
    some: { field: 'value' }
  };
  let ts = new Date();
  let client = mockClient(
    `/streams/${name}/events/${id}`,
    new Response(JSON.stringify(data), {
      headers: { Location: id, timestamp: ts.toISOString() }
    })
  );
  let stream = client.stream(name);
  return stream.get(id).then(event => {
    expect(event).toEqual({
      id,
      ts,
      event: data
    });
  });
});

test('Stream#getMeta gets metadata', () => {
  let name = 'sensor';
  let client = mockClient(`/streams/${name}/metadata`, {
    reading: 123,
    msg: 'hello there'
  });
  let stream = client.stream(name);
  return stream.getMeta().then(metadata => {
    expect(metadata.reading).toEqual(123);
    expect(metadata.msg).toEqual('hello there');
  });
});

/*
  ~~ Field ~~
*/

test('Field#stats', () => {
  let name = 'weather';
  let fieldName = 'humidity';
  let client = mockClient(`/streams/${name}/stats/${fieldName}`, {
    categorical: {},
    numerical: {
      count: 345,
      max: 0.98,
      mean: 0.647391304347826,
      min: 0.28,
      missing: 0,
      std: 0.15552099400186697
    }
  });
  let stream = client.stream(name);
  let field = new Field(stream, {
    id: '123',
    path: ['humidity'],
    start: new Date()
  });
  expect(field.toString()).toEqual(`${name}:${fieldName}`);
  return field.stats().then(stats => {
    expect(stats).toMatchObject({
      categorical: {},
      numerical: {
        count: 345,
        max: 0.98,
        mean: 0.647391304347826,
        min: 0.28,
        missing: 0,
        std: 0.15552099400186697
      }
    });
  });
});

/*
  ~~ Pattern ~~
*/

test('Pattern#search', () => {
  let name = 'my-pattern';
  let client = mockClient(`/patterns/${name}/search`, [
    { start: '2010-01-01T00:00:00Z', end: '2010-01-09T00:00:00Z' },
    { start: '2010-01-11T00:00:00Z', end: '2010-01-13T00:00:00Z' }
  ]);

  let pattern = new Pattern(client, {
    name
  });

  return pattern.search().then(spans => {
    expect(spans).toHaveLength(2);
    spans.forEach(span => {
      expect(span.start).toBeInstanceOf(Date);
      expect(span.end).toBeInstanceOf(Date);
    });
  });
});

test('Pattern#saveAs', () => {
  let name = 'my-pattern';
  let query = 'weather when temp > 0.5';

  let client = mockClient(
    `/patterns/${name}`,
    new Response({ pattern: query }, { status: 201 })
  );

  let pattern = new Pattern(client, {
    name: 'some-generated-id',
    anonymous: true,
    query
  });

  return pattern.saveAs(name).then(pattern => {
    expect(pattern.name).toEqual(name);
    expect(pattern.anonymous).toEqual(false);
  });
});

/*
  ~~ View ~~
*/

test('View#data', () => {
  let name = 'my-view';
  let client = mockClient(`/views/${name}/data`, {
    events: [
      {
        duration: null,
        event: {},
        id: 'abc123',
        stream: 'weather',
        ts: '2010-02-15T00:00:00Z'
      }
    ],
    streams: {}
  });

  let view = new View(client, {
    name
  });

  return view.data().then(data => {
    expect(data).toHaveLength(1);
    data.forEach(datum => {
      expect(datum.duration).toBeNull();
      expect(datum.event).toMatchObject({});
      expect(typeof datum.id).toBe('string');
      expect(typeof datum.stream).toBe('string');
      expect(datum.ts).toBeInstanceOf(Date);
    });
  });
});
