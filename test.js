
var sentenai = require('./sentenai.js');

var $ = sentenai.flare;

var client = new sentenai.Client({auth_key: ""});

var bos = $.stream({name: "weather-daily-boston"});

console.log($.ast(
  $.select()(
    bos({registers: {temp1: $.gt(50), state: $.or(false, true)}}).min({days: 3})
  )
));
