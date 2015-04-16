// table.js - the table class for ruddDB
var util = require("./util");
var types = require("./types");

module.exports = function Table (tbl_name, schema) {
    this.schema = schema;
    this.tbl_name = tbl_name;
    this.tuples = [];
    
    this._insert_tuple = function (tup) {
        // If the tuple doesn't match the schema, it's an error.
		if (!types.matches_schema(tup, schema)) {
            throw "Tuple doesn't match schema!";
        }
        this.tuples.push(tup);
    }
}
