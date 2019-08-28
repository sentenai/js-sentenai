const fetchMock = require('fetch-mock');
const { Stream, Field, Pattern } = require('../src/api.js');
const Client = require('../src/api.js').default;

function mockClient(matcher, response, opts) {
  let client = new Client({
    host: ''
  });
  client._fetch = fetchMock.sandbox().mock(matcher, response, opts);
  return client;
}

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
    });
  });
});

test('Client#patterns', () => {
  let client = mockClient('/patterns', [
    {
      anonymous: false,
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

  return client.pattern(query).then(pattern => {
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

  return client.pattern(query, name).then(pattern => {
    expect(pattern.query).toEqual(query);
    expect(pattern.name).toEqual(name);
    expect(pattern.anonymous).toEqual(false);
    expect(pattern.created).toBeInstanceOf(Date);
  });
});

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
