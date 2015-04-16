/* test.js - basic bootstrap tests for ruddDB */
var db = require("./db");
var util = require("./util");

function assert(condition, message) {
    if (!condition) {
        throw message || "Assertion failed";
    }
}

/* Check that the database exists */
assert(db._is_loaded(), "Database failed to load!");

/* Test inserts */
console.log("Testing inserts");
db.create_table("a", ["number"]);
db.insert("a", [3]);
db.insert("a", [4]);
db.insert("a", [5]);

/* Test selects */
console.log("Testing selects");
assert(util.array_compare(db.select("a"), [[3], [4], [5]]))
assert(util.array_compare(db.select("a", function (x) {return x > 3;}),
                          [[4], [5]]))

console.log("All tests passed!");
