/* global test, expect */
var { flare } = require('../');
var { stream, select, ast, lt, gte, gt, ne, or, all, any, event } = flare;

const expectAST = a => expect(JSON.parse(ast(a)));

test('select span', () => {
  const s = stream('S');
  expectAST(
    select()(
      s({ x: true })
    )
  ).toEqual({
    select: {
      type: 'span',
      op: '==',
      stream: { name: 'S' },
      path: [ 'event', 'x' ],
      arg: { type: 'bool', val: true }
    }
  });
});

test('any comparisons', () => {
  const moose = stream('moose');

  expectAST(
    any(
      moose({ x: lt(0) }),
      moose({ x: gte(3.141592653589793) }),
      moose({ b: ne(false) })
    )
  ).toEqual({
    type: 'any',
    conds: [
      {
        stream: { name: 'moose' },
        arg: { type: 'int', val: 0 },
        path: [ 'event', 'x' ],
        type: 'span',
        op: '<'
      },
      {
        stream: { name: 'moose' },
        arg: { type: 'double', val: 3.141592653589793 },
        path: [ 'event', 'x' ],
        type: 'span',
        op: '>='
      },
      {
        stream: { name: 'moose' },
        arg: { type: 'bool', val: false },
        path: [ 'event', 'b' ],
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
      s({ even: true })
        .then(s({ event: true }))
        .then(s({ event: { event: true } }))
        .then(s({ id: true }))
        .then(s({ '.id': true }))
        .then(s({ '.id': { '': true } }))
        .then(s({ true: { '真实': true } }))
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
      any(foo({ x: true }), bar({ y: true }))
        .then(baz({ z: true }))
        .then(all(qux({ 'α': true }), quux({ 'β': true })))
    )
  ).toEqual({
    select: {
      type: 'serial',
      conds: [
        {
          type: 'any',
          conds: [
            {op: '==', stream: {name: 'foo'}, path: ['event', 'x'], type: 'span', arg: {type: 'bool', val: true}},
            {op: '==', stream: {name: 'bar'}, path: ['event', 'y'], type: 'span', arg: {type: 'bool', val: true}}
          ]
        },
        {
          op: '==',
          stream: {name: 'baz'},
          path: ['event', 'z'],
          type: 'span',
          arg: {type: 'bool', val: true}
        },
        {
          type: 'all',
          conds: [
            {op: '==', stream: {name: 'qux'}, path: ['event', 'α'], type: 'span', arg: {type: 'bool', val: true}},
            {op: '==', stream: {name: 'quux'}, path: ['event', 'β'], type: 'span', arg: {type: 'bool', val: true}}
          ]
        }
      ]
    }
  });
});

test('or', () => {
  const s = stream('s');
  const t = stream('t');

  expectAST(
    select()(
      or(s({ x: true }), t({ x: true }))
    )
  ).toEqual({
    select: {
      expr: '||',
      args: [
         { type: 'span', op: '==', stream: {name: 's'}, path: ['event', 'x'], arg: {type: 'bool', val: true} },
         { type: 'span', op: '==', stream: {name: 't'}, path: ['event', 'x'], arg: {type: 'bool', val: true} }
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
        s({ x: true }).min({ years: 1, months: 1 }),
        t({ x: true }).after({ minutes: 11 }).within({ seconds: 13 })
      ).max({ weeks: 1 })
    )
  ).toEqual({
    select: {
      expr: '||',
      args: [
        {
          type: 'span',
          op: '==',
          stream: {name: 's'},
          path: ['event', 'x'],
          arg: {type: 'bool', val: true},
          for: { 'at-least': { years: 1, months: 1 } }
        },
        {
          type: 'span',
          op: '==',
          stream: {name: 't'},
          path: ['event', 'x'],
          arg: {type: 'bool', val: true},
          after: {minutes: 11},
          within: {seconds: 13}
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
      s({ x: lt(0) }).then(
        s({ x: 0 }).then(
          s({ x: gt(0) }).within({ seconds: 1 })
        ).within({ seconds: 2 })
      )
    )
  ).toEqual({
    select: {
      type: 'serial',
      conds: [
        {
          type: 'span',
          op: '<',
          stream: {name: 'S'},
          path: ['event', 'x'],
          arg: {type: 'int', val: 0}
        },
        {
          type: 'serial',
          conds: [
            {
              type: 'span',
              op: '==',
              stream: {name: 'S'},
              path: ['event', 'x'],
              arg: {type: 'int', val: 0}
            },
            {
              type: 'span',
              op: '>',
              stream: {name: 'S'},
              path: ['event', 'x'],
              arg: {type: 'int', val: 0},
              within: {seconds: 1}
            }
          ],
          within: {seconds: 2}
        }
      ]
    }
  });
});

test('switches', () => {
  const s = stream('S');

  expectAST(
    select()(
      s(event({ x: lt(0.1) }).then({ x: gt(0) }))
    )
  ).toEqual({
    select: {
      type: 'switch',
      stream: { name: 'S' },
      conds: [
        {
          op: '<',
          arg: { type: 'double', val: 0.1 },
          type: 'span',
          path: [ 'event', 'x' ]
        },
        {
          op: '>',
          arg: { type: 'int', val: 0 },
          type: 'span',
          path: [ 'event', 'x' ]
        }
      ]
    }
  });
});

// TODO: test('stream filters', () => {})
