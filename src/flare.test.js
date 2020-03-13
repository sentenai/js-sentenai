const {
  select,
  ast,
  lt,
  gte,
  gt,
  ne,
  and,
  or,
  all,
  any,
  event,
  filter,
  during
} = require('../index.js').default.flare;
const { Stream } = require('../src/api.js');

function stream(name, filters) {
  return new Stream(null, name, {}, filters);
}

const expectAST = a => expect(JSON.parse(ast(a)));

test('select span', () => {
  const s = stream('S');
  expectAST(select()(s.when({ x: true }))).toEqual({
    select: {
      type: 'span',
      op: '==',
      stream: { name: 'S' },
      path: ['event', 'x'],
      arg: { type: 'bool', val: true }
    }
  });
});

test('any comparisons', () => {
  const moose = stream('moose');

  expectAST(
    any(
      moose.when({ x: lt(0) }),
      moose.when({ x: gte(3.141592653589793) }),
      moose.when({ b: ne(false) })
    )
  ).toEqual({
    type: 'any',
    conds: [
      {
        stream: { name: 'moose' },
        arg: { type: 'double', val: 0 },
        path: ['event', 'x'],
        type: 'span',
        op: '<'
      },
      {
        stream: { name: 'moose' },
        arg: { type: 'double', val: 3.141592653589793 },
        path: ['event', 'x'],
        type: 'span',
        op: '>='
      },
      {
        stream: { name: 'moose' },
        arg: { type: 'bool', val: false },
        path: ['event', 'b'],
        type: 'span',
        op: '!='
      }
    ]
  });
});

test('stream access', () => {
  const s = stream('S');
  expectAST(
    select()(
      s
        .when({ even: true })
        .then(s.when({ event: true }))
        .then(s.when({ event: { event: true } }))
        .then(s.when({ id: true }))
        .then(s.when({ '.id': true }))
        .then(s.when({ '.id': { '': true } }))
        .then(s.when({ true: { 真实: true } }))
    )
  ).toEqual({
    select: {
      type: 'serial',
      conds: [
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', 'even'],
          stream: { name: 'S' }
        },
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', 'event'],
          stream: { name: 'S' }
        },
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', 'event', 'event'],
          stream: { name: 'S' }
        },
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', 'id'],
          stream: { name: 'S' }
        },
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', '.id'],
          stream: { name: 'S' }
        },
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', '.id', ''],
          stream: { name: 'S' }
        },
        {
          type: 'span',
          op: '==',
          arg: { type: 'bool', val: true },
          path: ['event', 'true', '\u771f\u5b9e'],
          stream: { name: 'S' }
        }
      ]
    }
  });
});

test('all and any in serial', () => {
  const foo = stream('foo');
  const bar = stream('bar');
  const baz = stream('baz');
  const qux = stream('qux');
  const quux = stream('quux');

  expectAST(
    select()(
      any(foo.when({ x: true }), bar.when({ y: true }))
        .then(baz.when({ z: true }))
        .then(all(qux.when({ α: true }), quux.when({ β: true })))
    )
  ).toEqual({
    select: {
      type: 'serial',
      conds: [
        {
          type: 'any',
          conds: [
            {
              op: '==',
              stream: { name: 'foo' },
              path: ['event', 'x'],
              type: 'span',
              arg: { type: 'bool', val: true }
            },
            {
              op: '==',
              stream: { name: 'bar' },
              path: ['event', 'y'],
              type: 'span',
              arg: { type: 'bool', val: true }
            }
          ]
        },
        {
          op: '==',
          stream: { name: 'baz' },
          path: ['event', 'z'],
          type: 'span',
          arg: { type: 'bool', val: true }
        },
        {
          type: 'all',
          conds: [
            {
              op: '==',
              stream: { name: 'qux' },
              path: ['event', 'α'],
              type: 'span',
              arg: { type: 'bool', val: true }
            },
            {
              op: '==',
              stream: { name: 'quux' },
              path: ['event', 'β'],
              type: 'span',
              arg: { type: 'bool', val: true }
            }
          ]
        }
      ]
    }
  });
});

test('or', () => {
  const s = stream('s');
  const t = stream('t');

  expectAST(select()(or(s.when({ x: true }), t.when({ x: true })))).toEqual({
    select: {
      expr: '||',
      args: [
        {
          type: 'span',
          op: '==',
          stream: { name: 's' },
          path: ['event', 'x'],
          arg: { type: 'bool', val: true }
        },
        {
          type: 'span',
          op: '==',
          stream: { name: 't' },
          path: ['event', 'x'],
          arg: { type: 'bool', val: true }
        }
      ]
    }
  });
});

test('relative span', () => {
  const s = stream('s');
  const t = stream('t');

  expectAST(
    select()(
      or(
        s.when({ x: true }).min({ years: 1, months: 1 }),
        t
          .when({ x: true })
          .after({ minutes: 11 })
          .within({ seconds: 13 })
      ).max({ weeks: 1 })
    )
  ).toEqual({
    select: {
      expr: '||',
      args: [
        {
          type: 'span',
          op: '==',
          stream: { name: 's' },
          path: ['event', 'x'],
          arg: { type: 'bool', val: true },
          for: { 'at-least': { years: 1, months: 1 } }
        },
        {
          type: 'span',
          op: '==',
          stream: { name: 't' },
          path: ['event', 'x'],
          arg: { type: 'bool', val: true },
          after: { minutes: 11 },
          within: { seconds: 13 }
        }
      ],
      for: { 'at-most': { weeks: 1 } }
    }
  });
});

test('nested relative spans', () => {
  const s = stream('S');

  expectAST(
    select()(
      s.when({ x: lt(0) }).then(
        s
          .when({ x: 0 })
          .then(s.when({ x: gt(0) }).within({ seconds: 1 }))
          .within({ seconds: 2 })
      )
    )
  ).toEqual({
    select: {
      type: 'serial',
      conds: [
        {
          type: 'span',
          op: '<',
          stream: { name: 'S' },
          path: ['event', 'x'],
          arg: { type: 'double', val: 0 }
        },
        {
          type: 'serial',
          conds: [
            {
              type: 'span',
              op: '==',
              stream: { name: 'S' },
              path: ['event', 'x'],
              arg: { type: 'double', val: 0 }
            },
            {
              type: 'span',
              op: '>',
              stream: { name: 'S' },
              path: ['event', 'x'],
              arg: { type: 'double', val: 0 },
              within: { seconds: 1 }
            }
          ],
          within: { seconds: 2 }
        }
      ]
    }
  });
});

test('switches', () => {
  const s = stream('S');

  expectAST(select()(s.when(event({ x: lt(0.1) }).then({ x: gt(0) })))).toEqual(
    {
      select: {
        type: 'switch',
        stream: { name: 'S' },
        conds: [
          {
            op: '<',
            arg: { type: 'double', val: 0.1 },
            type: 'span',
            path: ['event', 'x']
          },
          {
            op: '>',
            arg: { type: 'double', val: 0 },
            type: 'span',
            path: ['event', 'x']
          }
        ]
      }
    }
  );
});

test('stream filters', () => {
  const s = stream('S', { season: 'summer' });

  expectAST(
    select()(and(s.when({ temperature: gte(77.5) }), s.when({ sunny: true })))
  ).toEqual({
    select: {
      expr: '&&',
      args: [
        {
          type: 'span',
          op: '>=',
          stream: {
            name: 'S',
            filter: {
              op: '==',
              path: ['event', 'season'],
              arg: { type: 'string', val: 'summer' }
            }
          },
          path: ['event', 'temperature'],
          arg: { type: 'double', val: 77.5 }
        },
        {
          type: 'span',
          op: '==',
          stream: {
            name: 'S',
            filter: {
              op: '==',
              path: ['event', 'season'],
              arg: { type: 'string', val: 'summer' }
            }
          },
          path: ['event', 'sunny'],
          arg: { type: 'bool', val: true }
        }
      ]
    }
  });
});

test('or stream filters', () => {
  const s = stream(
    'S',
    or(filter({ season: 'summer' }), filter({ season: 'winter' }))
  );

  expectAST(select()(s.when({ sunny: true }))).toEqual({
    select: {
      type: 'span',
      op: '==',
      arg: { type: 'bool', val: true },
      path: ['event', 'sunny'],
      stream: {
        name: 'S',
        filter: {
          expr: '||',
          args: [
            {
              op: '==',
              arg: { type: 'string', val: 'summer' },
              path: ['event', 'season']
            },
            {
              op: '==',
              arg: { type: 'string', val: 'winter' },
              path: ['event', 'season']
            }
          ]
        }
      }
    }
  });
});

test('multiple conditions is shorthand for &&', () => {
  const s = stream('S');
  const t = stream('T');

  expectAST(select()(s.when({ sunny: true }), t.when({ happy: true }))).toEqual(
    select()(and(s.when({ sunny: true }), t.when({ happy: true }))).ast
  );
});

test('specifying start and end', () => {
  const s = stream('S');

  const cond = s.when({ sunny: true });
  const base = cond.ast;

  const start = '2017-06-01T00:00:00+00:00';
  const end = '2017-06-12T00:00:00+00:00';

  expectAST(select({ start, end })(cond)).toEqual({
    between: [start, end],
    select: base
  });

  expectAST(select({ start })(cond)).toEqual({
    after: start,
    select: base
  });

  expectAST(select({ end })(cond)).toEqual({
    before: end,
    select: base
  });
});

test('during', () => {
  const s = stream('S');

  expectAST(
    select()(during(s.when({ foo: 'bar' }), s.when({ baz: gt(1.5) })))
  ).toEqual({
    select: {
      type: 'during',
      conds: [
        {
          op: '==',
          arg: {
            type: 'string',
            val: 'bar'
          },
          type: 'span',
          path: ['event', 'foo'],
          stream: {
            name: 'S'
          }
        },
        {
          op: '>',
          arg: {
            type: 'double',
            val: 1.5
          },
          type: 'span',
          path: ['event', 'baz'],
          stream: {
            name: 'S'
          }
        }
      ]
    }
  });
});
