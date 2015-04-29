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

    /* transform_pred takes a predicate which is a function on the table's
       columns, and transforms it to a function that can be directly applied
       to tuples in the table. */
    this.transform_pred = function (pred) {
        var pred_str = pred.toString();

        /* Find the relevant parts of the function string */
        var arg_start = pred_str.indexOf("(") + 1;
        var arg_end = pred_str.indexOf(")");
        var body_start = pred_str.indexOf("{") + 1;
        var body_end = pred_str.length - 1;

        /* Break the args out of the argument list, and get the corresponding
           indices of the column names in the table */
        var args = pred_str.substring(arg_start, arg_end).split(",")
        args = args.map(function (x) {return x.trim();});
        var inds = args.map(function (x)
                               {return schema.get_index_of_col(x).toString();});

        /* Break the body of the function out, and take care of the relevant
           replacements, from column names to indexing into the tuple */
        var body = pred_str.substring(body_start, body_end);
        for (var i = 0; i < args.length; i++) {
            var arg = args[i];
            var rep_string = "tup[" + inds[i] + "]";
            body = body.replace(arg, rep_string);
        }

        /* Reconstruct the original function and return it. */
        eval("pred = function (tup) {" + body + "}");
        return pred;
    }
}
