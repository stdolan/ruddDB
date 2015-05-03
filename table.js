// table.js - the table class for ruddDB
var Schema = require("./schema");
var util = require("./util");

module.exports = function Table (tbl_name, schema) {
    this.schema = schema;
    this.tbl_name = tbl_name;
    this.tuples = [];
    
    /* insert_tuple checks if tup matches the table's schema, then pushes
       the tuple into the table's tuple array if it does. */
    this.insert_tuple = function (tup) {
        // If the tuple doesn't match the schema, it's an error.
		if (!schema.matches_tuple(tup)) {
            throw "Tuple doesn't match schema!";
        }
        this.tuples.push(tup);
    }
	
	/* delete_pred deletes tuples from the table which satisfy the predicate.
	   If the predicate is empty, all tuples are deleted. */
	this.delete_tuples = function (pred_str) {
	    // If no predicate was supplied, delete everything.
	    if (pred_str === undefined) {
		    pred_str = "true";
		}
		
		pred = util.transform_pred(pred_str, schema);
		this.tuples = this.tuples.filter(function (t) { return !pred(t); });
	}

    this.update_tuples = function(mut_str, pred_str) {
        /* Transform the given functions, then just update each record
           one by one. */
        mut = util.transform_mut(mut_str, schema);
        pred = util.transform_pred(pred_str, schema);
        var tuple;
        for (var i = 0; i < this.tuples.length; i++) {
            tuple = this.tuples[i];
            if (pred(tuple)) {
                mut(tuple);
                this.tuples[i] = tuple;
            }
        }
    }
}
