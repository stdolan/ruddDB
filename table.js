// table.js - the table class for ruddDB
var Schema = require("./schema");
// Solely required for make_schema
var types = require("./types");

// We should probably move this to schema.js, ie have the schema constructor take a variable
// (in terms of type) argument. Or maybe util?
// Makes a schema object out of an SQL style schema declaration.
function make_schema(s) {
    var names = [], types = [];

    s.split(",")
    // Get each table/type on its own
    .map(function(s) {
        return s.match(/\s*(\S)+\s+(\S+)\s*/).slice(1);
    })
    // Make the arrays to pass to schema
    .map(function (t) {
        names.push(t[0]);
        types.push(types.map_type(t[1]));
    });

    return new Schema(names, types);
}

module.exports = function Table (tbl_name, schema) {
    if (typeof schema === "string")
        schema = make_schema(schema);
	
    this.schema = schema;
    this.tbl_name = tbl_name;
    this.tuples = [];
    
    /* insert_tuple checks if tup matches the table's schema, then pushes
       the tuple into the table's tuple array if it does. */
    this.insert_tuple = function (tup) {
        // If the tuple doesn't match the schema, it's an error.
		if (!schema.matches_tuple(tup)) {
            console.log(tup);
            throw "Tuple doesn't match schema!";
        }
        this.tuples.push(tup);
    }
	
	/* delete_pred deletes tuples from the table which satisfy the predicate.
	   If the predicate is empty, all tuples are deleted. */
	this.delete_tuples = function (pred) {
		this.tuples = this.tuples.filter(function (t) { return !pred(t); });
	}

    this.update_tuples = function(mut, pred) {
        /* Transform the given functions, then just update each record
           one by one. */

        var tuple;
        for (var i = 0; i < this.tuples.length; i++) {
            tuple = this.tuples[i];
            if (pred(tuple))
                mut(tuple);
        }
    }

    // Returns schema, tbl_name, and tuples
    this._get_data = function() {
        return ["table name:", this.tbl_name,
            "schema names:", this.schema.names,
            "schema types:", this.schema.types,
            "tuples:", this.tuples];
    }
}
