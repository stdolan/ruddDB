/* test.js - basic bootstrap tests for ruddDB */
var db = require("./db");
var Schema = require("./schema");
var Table = require("./table");
var nodes = require("./nodes");
var parser = require("./parser");
var types = require("./types");
var util = require("./util");

function assert(condition, message) {
    if (!condition) {
        throw message || "Assertion failed";
    }
}

// TODO test unordered results somehow?
function test_plan(plan, expected) {
	var actual = [];
	var tup = plan.next_tuple();
	while(tup !== null) {
		actual.push(tup);
		tup = plan.next_tuple();
	}
	
	if(!util.array_deep_eq(actual, expected)) {
		console.log("Results differ from expected!");
		console.log(actual);
		console.log(expected);
		assert(false);
	}
}

/* Check that the database exists */
assert(db._is_loaded(), "Database failed to load!");

/* Test typing */
console.log("Testing types");
var sch = new Schema(['Foo', 'Bar'], [types.INTEGER, types.STRING]);
assert( sch.matches_tuple([0, 'w']));
assert(!sch.matches_tuple([0]));
assert(!sch.matches_tuple([0, 'w', 4]));
assert(!sch.matches_tuple(['w', 0]));

// TODO test the transform functions

/* Testing nodes */
var table_a = new Table('Animals', new Schema(['name'], [types.STRING]));
var table_b = new Table('Numbers', new Schema(['len'], [types.INTEGER]));
var table_c = new Table('Other', new Schema(['len'], [types.INTEGER]));

table_a.insert_tuple(['dog']);
table_a.insert_tuple(['cat']);
table_a.insert_tuple(['giraffe']);

table_b.insert_tuple([3]);
table_b.insert_tuple([7]);
table_b.insert_tuple([2]);

table_c.insert_tuple([4]);
table_c.insert_tuple([7]);

console.log("Testing select nodes");
var plan = new nodes.SelectNode(new nodes.TableNode(table_a),
    function (tup) { return tup[0].length === 3; });
test_plan(plan, [['dog'], ['cat']]);

console.log("Testing join nodes");
plan = new nodes.JoinNode(new nodes.TableNode(table_a),
                          new nodes.TableNode(table_b));
test_plan(plan, [['dog', 3], ['dog', 7], ['dog', 2],
                 ['cat', 3], ['cat', 7], ['cat', 2],
				 ['giraffe', 3], ['giraffe', 7], ['giraffe', 2]]);
				 
console.log("Testing union nodes");
plan = new nodes.UnionNode(new nodes.TableNode(table_b),
                           new nodes.TableNode(table_c));
test_plan(plan, [[3], [7], [2], [4], [7]]);

console.log("Testing project nodes");
plan = new nodes.ProjectNode(
    new nodes.TableNode(table_b),
    new Schema(['squared', 'even?'], [types.INTEGER, types.BOOLEAN]),
	function (tup) { var x = tup[0]; return [x * x, x % 2 === 0]; });
test_plan(plan, [[9, false], [49, false], [4, true]]);

console.log("Testing folding nodes");
plan = new nodes.FoldingNode(
    new nodes.TableNode(table_a),
	function (tup) { return [tup[0].length]; },
	function (acc, tup) { if(acc === undefined) { acc = ['']; }
	                      return [acc[0] + tup[0]]; });
test_plan(plan, [[3, 'dogcat'], [7, 'giraffe']]);

// TODO test queries! db.select is different from SelectNode...

/* Test inserts */
console.log("Testing inserts");
db.create('a', new Schema(['Num'], [types.INTEGER]));
db.insert('a', [[3], [4], [5]]);
assert(db.eval(db.select('a')), [[3], [4], [5]]);
db.insert('a', [[7]]);
assert(db.eval(db.select('a')), [[3], [4], [5], [7]]);

/* Test updates */
console.log("Testing updates");
db.update("a", "Num = 11", "Num >= 5");
assert(util.array_deep_eq(db.eval(db.select("a")), [[3], [4], [11], [11]]));
db.update("a", "Num = Num % 3");
assert(util.array_deep_eq(db.eval(db.select("a")), [[0], [1], [2], [2]]));

/* Test deletes */
console.log("Testing deletes");
db.delete("a", "Num > 1");
assert(util.array_deep_eq(db.eval(db.select("a")), [[0], [1]]),
		"Failed to delete with predicate!")
db.delete("a", "Num == 0");
assert(util.array_deep_eq(db.eval(db.select("a")), [[1]]),
		"Failed to delete with predicate!")
db.delete("a");
assert(util.array_deep_eq(db.eval(db.select("a")), []),
        "Failed to delete without predicate!")
		
console.log("All tests passed!");
