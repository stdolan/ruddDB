var db = require('../db.js');
var Schema = require("../schema");
var Table = require("../table");
var nodes = require("../nodes");
var parser = require("../parser");
var types = require("../types");
var util = require("../util");
var repl = require("repl");
var fs = require('fs');
var parse = require('csv-parse');

var input = fs.createReadStream("./Sacramentorealestatetransactions.csv");
var data = []

var parser = parse();
parser.on('readable', function(){
  while(record = parser.read()){
    data.push(record);
  }
});

input.pipe(parser);
sch_types = Array(13).join("a").split('').map(function(){return types.STRING});
r = repl.start("node> ");
r.context.data = data;
r.context.sch_types = sch_types;
r.context.db = db;
r.context.Schema = Schema;
r.context.Table = Table;
r.context.nodes = nodes;
r.context.parser = parser;
r.context.types = types;
r.context.util = util;

