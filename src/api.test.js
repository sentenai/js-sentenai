const fetchMock = require('fetch-mock');
const { Stream, Field } = require('../src/api.js');
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
    let first = streams[0];
    expect(first).toBeInstanceOf(Stream);
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
  let stream = new Stream(client, name);
  return stream.fields().then(fields => {
    expect(fields).toHaveLength(2);
    let first = fields[0];
    expect(first).toBeInstanceOf(Field);
  });
});
