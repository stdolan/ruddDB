// table.js - the table class for ruddDB
var Schema = require("./schema");

module.exports = function Table (tbl_name, schema) {
    this.schema = schema;
    this.tbl_name = tbl_name;
    this.tuples = [];
    
    this._insert_tuple = function (tup) {
        // If the tuple doesn't match the schema, it's an error.
		if (!schema.matches_tuple(tup)) {
            throw "Tuple doesn't match schema!";
        }
        this.tuples.push(tup);
    }
}
