require('es6-shim');

var moment;
try {
  moment = require('moment-timezone');
} catch (err) {}

var FlareException = function () { };

function mkCondTree (stream, path, cond) {
  if (cond instanceof AnyCmp) {
    let alts = [];
    for (let i = 0; i < cond.vals.length; i++) {
      alts.push(mkCondTree(stream, path, cond.vals[i]));
    }
    return new Or(alts);
  } else if (cond instanceof AllCmp) {
    let alts = [];
    for (let i = 0; i < cond.vals.length; i++) {
      alts.push(mkCondTree(stream, path, cond.vals[i]));
    }
    return new And(alts);
  } else if (cond instanceof Cmp) {
    return new Cond(path, cond.op, cond.val, stream);
  } else {
    return new Cond(path, '==', cond, stream);
  }
}

function mkSpans (stream, conds, path) {
  if (typeof path === 'undefined') { path = []; }
  var ands = [];
  for (var key in conds) {
    var p = path.slice();
    p.push(key);
    var val = conds[key];
    switch (typeof (val)) {
      case 'object':
        if (val instanceof Cmp) {
          ands.push(new Cond(p, val.op, val.val, stream));
        } else if (val instanceof AnyCmp) {
          ands.push(mkCondTree(stream, p, val));
        } else if (val instanceof AllCmp) {
          ands.push(mkCondTree(stream, p, val));
        } else {
          ands.push(mkSpans(stream, val, p));
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
    var c = {'select': this.serial.ast};
    if (this.start) {
      c['start'] = this.start;
    }
    if (this.end) {
      c['end'] = this.end;
    }
    return c;
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
  constructor (name) {
    this.name = name;
  }

  get ast () {
    return {'name': this.name};
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
      'conds': this.sequence.map(function (a) { return a.ast; })
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
      'conds': this.conds.map(function (a) { return a.ast; })
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
      'type': 'any',
      'conds': this.conds.map(function (a) { return a.ast; })
    };
  }
}

class Spacing {
  constructor (cond, after, within) {
    this.cond = cond;
    this.after = after;
    this.within = within;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    var c = this.cond.ast;
    if (this.after) { c['after'] = this.after.ast; }
    if (this.within) { c['within'] = this.within.ast; }
    return c;
  }
}

class Width {
  constructor (cond, min, max) {
    this.cond = cond;
    this.min = min;
    this.max = max;
  }

  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }

  get ast () {
    var c = this.cond.ast;
    // TODO: min/max are instances of Delta, this comparison will always fail
    if (this.min && this.max && this.min === this.max) {
      c['for'] = this.min.ast;
    } else {
      if (this.min) { c['for'] = {'at-least': this.min.ast}; }
      if (this.max) { c['for'] = {'at-most': this.max.ast}; }
    }
    return c;
  }
}

class And {
  constructor (conds) {
    this.conds = conds;
  }
  get ast () {
    return {
      'type': '&&',
      'conds': this.conds.map(function (a) { return a.ast; })
    };
  }
  after (delta) { return new Spacing(this, new Delta(delta)); }
  within (delta) { return new Spacing(this, undefined, new Delta(delta)); }
  min (delta) { return new Width(this, new Delta(delta)); }
  max (delta) { return new Width(this, undefined, new Delta(delta)); }
  then (cond) { return new Serial([this, cond]); }
}

class Or {
  constructor (alternatives) {
    this.conds = alternatives;
  }
  get ast () {
    return {
      'type': '||',
      'conds': this.conds.map(function (a) { return a.ast; })
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

class AnyCmp {
  constructor (vals) {
    this.vals = vals;
  }
}

class AllCmp {
  constructor (vals) {
    this.vals = vals;
  }
}

var Flare = {};

Flare.select = function (start, end) {
  return function () {
    if (arguments.length === 0) {
      throw FlareException('select * not supported yet');
    } else if (arguments.length === 1) {
      return new Select(arguments[0], start, end);
    } else {
      return new Select(new Serial(Array.from(arguments)), start, end);
    }
  };
};

Flare.stream = function (args) {
  var name;
  switch (typeof (args)) {
    case 'object':
      name = args.name;
      break;
    case 'string':
      name = args;
      break;
    default:
      throw new FlareException('bad stream arguments');
  }
  return function () {
    if (arguments.length < 1) {
      throw new FlareException("can't bind stream to nothing");
    } else if (arguments.length === 1) {
      /* SPAN */
      return mkSpans(new Stream(name), arguments[0], ['event']);
    } else {
      throw FlareException('switches not supported yet');
      /* SWITCH */
    }
    // new Stream(name);
  };
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
  } else if (arguments[0] instanceof Cond) {
    return new And(Array.from(arguments));
  } else if (arguments[0] instanceof And) {
    return new And(Array.from(arguments));
  } else if (arguments[0] instanceof Or) {
    return new And(Array.from(arguments));
  } else {
    return new AllCmp(Array.from(arguments));
  }
};

Flare.or = function () {
  if (arguments.length === 0) {
    throw new FlareException('and cannot have zero arguments');
  } else if (arguments.length === 1) {
    return arguments[0];
  } else if (arguments[0] instanceof Cond) {
    return new Or(Array.from(arguments));
  } else if (arguments[0] instanceof And) {
    return new Or(Array.from(arguments));
  } else if (arguments[0] instanceof Or) {
    return new Or(Array.from(arguments));
  } else {
    return new AnyCmp(Array.from(arguments));
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

Flare.ast = function (obj) { return JSON.stringify(obj.ast); };

module.exports = Flare;
