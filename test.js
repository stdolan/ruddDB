/* test.js - basic bootstrap tests for ruddDB */
var db = require("./db");
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
var schema = [types.INTEGER, types.STRING];
var tup1 = [0, 'w'];
var tup2 = [0];
var tup3 = [0, 'w', 4];
var tup4 = ['w', 0];

assert( types.matches_schema(tup1, schema));
assert(!types.matches_schema(tup2, schema));
assert(!types.matches_schema(tup3, schema));
assert(!types.matches_schema(tup4, schema));

/* Test inserts */
console.log("Testing inserts");
db.create_table("a", [types.INTEGER]);
db.insert("a", [3]);
db.insert("a", [4]);
db.insert("a", [5]);

/* Test selects */
console.log("Testing selects");
assert(util.array_compare(db.select("a"), [[3], [4], [5]]))
assert(util.array_compare(db.select("a", function (x) {return x > 3;}),
                          [[4], [5]]))

console.log("All tests passed!");
