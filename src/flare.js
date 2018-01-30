require('es6-shim');

var moment;
try {
  moment = require('moment-timezone');
} catch (err) {}

var FlareException = function () { };

function makeSpans (stream, conds, path) {
  if (typeof path === 'undefined') { path = []; }
  var ands = [];
  for (var key in conds) {
    var p = path.slice();
    p.push(key);
    var val = conds[key];
    switch (typeof val) {
      case 'object':
        if (val instanceof Cmp) {
          ands.push(new Cond(p, val.op, val.val, stream));
        } else {
          ands.push(makeSpans(stream, val, p));
        }
        break;
      default:
        ands.push(new Cond(p, '==', val, stream));
    }
  }
  if (ands.length === 0) {
    throw FlareException('`*` not supported yet');
  } else if (ands.length === 1) {
    return ands[0];
  } else {
    return new And(ands);
  }
}

class Select {
  constructor (serial, start, end) {
    this.serial = serial;
    this.start = start;
    this.end = end;
  }

  get ast () {
    var ast = {'select': this.serial.ast};
    if (this.start && this.end) {
      ast.between = [this.start, this.end];
    } else if (this.start) {
      ast.after = this.start;
    } else if (this.end) {
      ast.before = this.end;
    }
    return ast;
  }
}

class Delta {
  constructor (args) {
    this.years = args.years;
    this.months = args.months;
    this.weeks = args.weeks;
    this.days = args.days;
    this.hours = args.hours;
    this.minutes = args.minutes;
    this.seconds = args.seconds;
  }

  get ast () {
    var d = {};
    for (let key in this) {
      if (typeof this[key] !== 'undefined') {
        d[key] = this[key];
      }
    }
    return d;
  }
}

class Stream {
  constructor (name, _filter) {
    this.name = name;
    this._filter = _filter && _filter.constructor === Object ? new Filter(_filter) : _filter;
  }

  get ast () {
    const ast = { 'name': this.name };
    if (this._filter) {
      ast.filter = this._filter.ast;
    }
    return ast;
  }
}

class Filter {
  constructor (map) {
    this._map = map;
  }

  get ast () {
    return makeSpans(undefined, this._map, ['event']).ast;
  }
}

class Serial {
  constructor (sequence) {
    this.sequence = sequence;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { this.sequence.push(cond); return this; }

  get ast () {
    return {
      'type': 'serial',
      'conds': this.sequence.map(a => a.ast)
    };
  }
}

class Any {
  constructor (alternatives) {
    this.conds = alternatives;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    return {
      'type': 'any',
      'conds': this.conds.map(a => a.ast)
    };
  }
}

class All {
  constructor (alternatives) {
    this.conds = alternatives;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    return {
      'type': 'all',
      'conds': this.conds.map(a => a.ast)
    };
  }
}

class Spacing {
  constructor (cond, after, within) {
    this.cond = cond;
    this._after = after;
    this._within = within;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    var c = this.cond.ast;
    if (this._after) { c['after'] = this._after.ast; }
    if (this._within) { c['within'] = this._within.ast; }
    return c;
  }
}

class Width {
  constructor (cond, min, max) {
    this.cond = cond;
    this._min = min;
    this._max = max;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    var c = this.cond.ast;
    // TODO: min/max are instances of Delta, this comparison will always fail
    if (this._min && this._max && this._min === this._max) {
      c['for'] = this._min.ast;
    } else {
      if (this._min) { c['for'] = {'at-least': this._min.ast}; }
      if (this._max) { c['for'] = {'at-most': this._max.ast}; }
    }
    return c;
  }
}

class And {
  constructor (conds) {
    if (conds.length > 2) {
      this.conds = [conds[0], new And(conds.slice(1))];
    } else {
      this.conds = conds;
    }
  }
  get ast () {
    return {
      expr: '&&',
      args: this.conds.map(a => a.ast)
    };
  }
  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }
}

class Or {
  constructor (conds) {
    if (conds.length > 2) {
      this.conds = [conds[0], new Or(conds.slice(1))];
    } else {
      this.conds = conds;
    }
  }
  get ast () {
    return {
      expr: '||',
      args: this.conds.map(a => a.ast)
    };
  }
  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }
}

class Cond {
  constructor (path, op, value, stream) {
    this.path = path;
    this.op = op;
    this.value = value;
    this.stream = stream;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    var t;
    switch (typeof (this.value)) {
      case 'number':
        t = Number.isInteger(this.value) ? 'int' : 'double';
        break;
      case 'boolean':
        t = 'bool';
        break;
      case 'string':
        t = 'string';
        break;
      default:
        console.log('error in type');
    }

    var c = {
      'path': this.path,
      'op': this.op,
      'arg': {'type': t, 'val': this.value}
    };
    if (typeof this.stream !== 'undefined') {
      c['stream'] = this.stream.ast;
      c['type'] = 'span';
    }
    return c;
  }
}

class Cmp {
  constructor (op, val) {
    this.op = op;
    this.val = val;
  }
}

class Switch {
  constructor (conds) {
    if (Array.isArray(conds)) {
      this.conds = conds;
    } else {
      this.conds = [conds];
    }
  }

  then (cond) {
    return new Switch(this.conds.concat(cond));
  }

  get ast () {
    throw new FlareException('Must bind a switch to a stream before producing an AST');
  }
}

class BoundSwitch {
  constructor (stream, sw) {
    this.stream = stream;
    this.switch = sw;
  }

  get ast () {
    return {
      type: 'switch',
      stream: { name: this.stream.name },
      conds: this.switch.conds.map(cond => makeSpans(this.stream, cond, ['event']).ast).map(span => {
        delete span.stream;
        return span;
      })
    };
  }
}

var Flare = {};

Flare.select = function (options) {
  options || (options = {});

  return function () {
    if (arguments.length === 0) {
      throw FlareException('select * not supported yet');
    } else if (arguments.length === 1) {
      return new Select(arguments[0], options.start, options.end);
    } else {
      return new Select(new And(Array.from(arguments)), options.start, options.end);
    }
  };
};

Flare.stream = function (name, filters) {
  if (typeof name === 'object') {
    name = name.name;
  } else if (typeof name !== 'string') {
    throw new FlareException('bad stream arguments');
  }
  return function () {
    if (arguments.length < 1) {
      // TODO: this might be mixing responsibilities too much
      return name;
    } else if (arguments.length === 1) {
      if (arguments[0] instanceof Switch) {
        return new BoundSwitch(new Stream(name, filters), arguments[0]);
      } else {
        return makeSpans(new Stream(name, filters), arguments[0], ['event']);
      }
    } else {
      throw FlareException('Too many arguments provided to stream');
    }
  };
};

Flare.filter = function (map) {
  return new Filter(map);
};

Flare.event = function (cond) {
  return new Switch(cond);
};

/* parallel pattern match */
Flare.any = function () { return new Any(Array.from(arguments)); };
Flare.all = function () { return new All(Array.from(arguments)); };

/* versatile functions */
Flare.and = function () {
  if (arguments.length === 0) {
    throw new FlareException('and cannot have zero arguments');
  } else if (arguments.length === 1) {
    return arguments[0];
  } else {
    return new And(Array.from(arguments));
  }
};

Flare.or = function () {
  if (arguments.length === 0) {
    throw new FlareException('or cannot have zero arguments');
  } else if (arguments.length === 1) {
    return arguments[0];
  } else {
    return new Or(Array.from(arguments));
  }
};

/* comparison operators */
Flare.gt = function (val) { return new Cmp('>', val); };
Flare.lt = function (val) { return new Cmp('<', val); };
Flare.gte = function (val) { return new Cmp('>=', val); };
Flare.lte = function (val) { return new Cmp('<=', val); };
Flare.ne = function (val) { return new Cmp('!=', val); };
Flare.eq = function (val) { return new Cmp('==', val); };

/* time related functions */
Flare.utc = function (Y, m, d, H, M, S) { return new Date(Y, m || 1, d || 1, H || 0, M || 0, S || 0); };
Flare.tz = function (tz) {
  if (typeof moment !== 'undefined') {
    return function (Y, m, d, H, M, S) {
      return moment(new Date(Y, m || 1, d || 1, H || 0, M || 0, S || 0)).tz(tz);
    };
  } else {
    throw new FlareException('The function `Flare.tz` requires optional dependency `moment-tz` to be installed.');
  }
};

Flare.ast = function (obj) { return JSON.stringify(obj.ast, null, 2); };
Flare.FlareException = FlareException;

module.exports = Flare;
