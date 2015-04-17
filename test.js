/* test.js - basic bootstrap tests for ruddDB */
var db = require("./db");
var Schema = require("./schema");
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
var sch = new Schema(['Foos', 'Bars'], [types.INTEGER, types.STRING]);
var tup1 = [0, 'w'];
var tup2 = [0];
var tup3 = [0, 'w', 4];
var tup4 = ['w', 0];

assert( sch.matches_tuple(tup1));
assert(!sch.matches_tuple(tup2));
assert(!sch.matches_tuple(tup3));
assert(!sch.matches_tuple(tup4));

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
