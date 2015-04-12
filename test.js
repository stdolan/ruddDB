/* test.js - basic bootstrap tests for ruddDB */
var db = require("./db");

function assert(condition, message) {
    if (!condition) {
        throw message || "Assertion failed";
    }
}

/* Check that the database exists */
assert(db._is_loaded(), "Database failed to load!");

/* Try some basic creation and inserts */
db.create_table("a", ["number"]);
db.insert("a", [3]);
db.insert("a", [4]);
console.log("Trying to select some values, should be [[3], [4]]");
console.log(db.select("a"));
console.log("Trying to select some values, should be [[4]]");
console.log(db.select("a", function (x) {return x > 3;}))

console.log("All tests passed!");
