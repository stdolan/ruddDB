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

table_a.insert_tuple(['dog']);
table_a.insert_tuple(['cat']);
table_a.insert_tuple(['giraffe']);

table_b.insert_tuple([3]);
table_b.insert_tuple([7]);
table_b.insert_tuple([5]);

function test_func(tup) {
	return tup[0].length === tup[1];
}

var plan_str = parser.parse("SELECT(JOIN(table_a, table_b), `name.length === len`)");
eval("var plan = " + plan_str);
assert(util.array_eq(plan.next_tuple(), ['dog', 3]));
assert(util.array_eq(plan.next_tuple(), ['cat', 3]));
assert(util.array_eq(plan.next_tuple(), ['giraffe', 7]));
assert(plan.next_tuple() === null);

var sch = new Schema(['foo'], [types.INTEGER]);
var table_l = new Table('Left', sch);
var table_r = new Table('Right', sch);

for(var i = 0; i < 5; i++) {
	table_l.insert_tuple([i]);
	table_r.insert_tuple([i * 10]);
}

plan_str = parser.parse("UNION(table_l, table_r)");
eval("plan = " + plan_str);
							   
for(var i = 0; i < 5; i++)
	assert(util.array_eq(plan.next_tuple(), [i]));
for(var i = 0; i < 5; i++)
	assert(util.array_eq(plan.next_tuple(), [i * 10]));
assert(plan.next_tuple() === null);

/* Test folding node */
console.log("Testing aggregations");
var s = new Schema (['foo', 'bar'], [types.INTEGER, types.FLOAT])
var t = new Table('MyTable', s);
t.insert_tuple([0,1]);
t.insert_tuple([0,2]);
t.insert_tuple([0,4]);
t.insert_tuple([1,3]);
t.insert_tuple([1,3]);
t.insert_tuple([2,0]);
var tn = new nodes.TableNode(t);
function g (tup) { return [tup[0]]; }
function a1 (acc, tup) { if(acc === undefined) { return [1]; } return [acc[0] + 1]; }
function a2 (acc, tup) { if(acc === undefined) { return [tup[1]]; } return [acc[0] + tup[1]]; }

var fn1 = new nodes.FoldingNode(tn, g, a1);
assert(util.array_deep_eq(fn1.next_tuple(), [0, 3]));
assert(util.array_deep_eq(fn1.next_tuple(), [1, 2]));
assert(util.array_deep_eq(fn1.next_tuple(), [2, 1]));
assert(fn1.next_tuple() === null);

tn.reset();
var fn2 = new nodes.FoldingNode(tn, g, a2);
assert(util.array_deep_eq(fn2.next_tuple(), [0, 7]));
assert(util.array_deep_eq(fn2.next_tuple(), [1, 6]));
assert(util.array_deep_eq(fn2.next_tuple(), [2, 0]));
assert(fn2.next_tuple() === null);

/* Test inserts */
console.log("Testing inserts");
var sch2 = new Schema(['Num'], [types.INTEGER]);
db.create("a", sch2);
db.insert("a", [3]);
db.insert("a", [4]);
db.insert("a", [5]);

/* Test selects */
console.log("Testing selects");
assert(util.array_deep_eq(db.select("a"), [[3], [4], [5]]))
assert(util.array_deep_eq(db.select("a", "Num > 3"), [[4], [5]]))
assert(util.array_deep_eq(db.select("a", function (Num) {return Num > 3;}),
                                    [[4], [5]]))				  

/* Test updates */
console.log("Testing updates");
db.update("a", "Num = 6", "Num > 4");
assert(util.array_deep_eq(db.select("a"), [[3], [4], [6]]))
db.update("a", function (Num) {Num = 5;}, function (Num) {return Num > 5});
assert(util.array_deep_eq(db.select("a"), [[3], [4], [5]])) 

/* Test deletes */
console.log("Testing deletes");
db.delete("a", "Num > 4");
assert(util.array_deep_eq(db.select("a"), [[3], [4]]),
		"Failed to delete with predicate!")
db.delete("a", function (Num) {return Num > 3;});
assert(util.array_deep_eq(db.select("a"), [[3]]),
		"Failed to delete with predicate!")
db.delete("a");
assert(util.array_deep_eq(db.select("a"), []),
        "Failed to delete without predicate!")
		
console.log("All tests passed!");
