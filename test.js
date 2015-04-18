/* test.js - basic bootstrap tests for ruddDB */
var db = require("./db");
var Schema = require("./schema");
var Table = require("./table");
var types = require("./types");
var util = require("./util");
var nodes = require("./nodes");

function assert(condition, message) {
    if (!condition) {
        throw message || "Assertion failed";
    }
}

/* Check that the database exists */
assert(db._is_loaded(), "Database failed to load!");

/* Test typing */
console.log("Testing types");
var sch = new Schema(['Foo', 'Bar'], [types.INTEGER, types.STRING]);
var tup1 = [0, 'w'];
var tup2 = [0];
var tup3 = [0, 'w', 4];
var tup4 = ['w', 0];

assert( sch.matches_tuple(tup1));
assert(!sch.matches_tuple(tup2));
assert(!sch.matches_tuple(tup3));
assert(!sch.matches_tuple(tup4));

/* Test pipeline */
console.log("Testing pipeline");
var table_a = new Table('Animals', new Schema(['name'], [types.STRING]));
var table_b = new Table('Numbers', new Schema(['len'], [types.INTEGER]));

table_a._insert_tuple(['dog']);
table_a._insert_tuple(['cat']);
table_a._insert_tuple(['giraffe']);

table_b._insert_tuple([3]);
table_b._insert_tuple([7]);
table_b._insert_tuple([5]);

var plan = new nodes.SelectNode(new nodes.JoinNode(new nodes.TableNode(table_a),
        new nodes.TableNode(table_b)), function (tup) { return tup[0].length === tup[1]; })
	
assert(util.array_compare(plan.nextTuple(), ['dog', 3]));
assert(util.array_compare(plan.nextTuple(), ['cat', 3]));
assert(util.array_compare(plan.nextTuple(), ['giraffe', 7]));
assert(plan.nextTuple() === null);

/* Test inserts */
console.log("Testing inserts");
var sch2 = new Schema(['Column'], [types.INTEGER]);
db.create_table("a", sch2);
db.insert("a", [3]);
db.insert("a", [4]);
db.insert("a", [5]);

/* Test selects */
console.log("Testing selects");
assert(util.array_compare(db.select("a"), [[3], [4], [5]]))
assert(util.array_compare(db.select("a", function (x) {return x > 3;}),
                          [[4], [5]]))						  
						  
console.log("All tests passed!");
