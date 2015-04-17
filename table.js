// table.js - the table class for ruddDB
var Schema = require("./schema");
var util = require("./util");

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

    this._transform_pred = function (pred) {
        var pred_str = pred.toString();

        var arg_start = pred_str.indexOf("(") + 1;
        var arg_end = pred_str.indexOf(")");
        var body_start = pred_str.indexOf("{") + 1;
        var body_end = pred_str.indexOf("}");

        var args = pred_str.substring(arg_start, arg_end).split(",")
        args = args.map(function (x) {return x.trim();});
        var inds = args.map(function (x)
                               {return schema.get_index_of_col(x).toString();});
        var body = pred_str.substring(body_start, body_end);
        
        for (var i = 0; i < args.length; i++) {
            var arg = args[i];
            var rep_string = "tup[" + inds[i] + "]";
            body = body.replace(arg, rep_string);
        }

        eval("pred = function (tup) {" + body + "}");
        return pred;
    }
}
