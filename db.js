// db.js - The main database object in ruddDB
var Table = require("./table");
var util = require("./util");

var tables = {};


/* Creates a table. Equivalent to SQL: CREATE tbl_name (schema) */
exports.create = function (tbl_name, schema) {
    tables.tbl_name = new Table(tbl_name, schema);
}

/* Inserts a row into a table.
   Equivalent to SQL: INSERT INTO tbl_name VALUE tup */
exports.insert = function (tbl_name, tup) {
    if (tables.tbl_name !== undefined)
        tables.tbl_name.insert_tuple(tup);
    else
        throw "Table " + tbl_name + " not found!";
}

/* Deletes tuples from a table that satisfy the given predicate
   Equivalent to SQL: DELETE FROM tbl_name WHERE pred */
exports._delete = function (tbl_name, pred_str) {
    if (tables.tbl_name !== undefined)
	    tables.tbl_name.delete_pred(pred_str);
	else
	    throw "Table " + tbl_name + " not found!";
}
/* Selects rows from a table.
   Equivalent to SQL: SELECT * FROM tbl_name WHERE pred */
exports.select = function (tbl_name, pred_str) {
    var table = tables.tbl_name;

    // If we didn't supply a predicate, return everything.
    if (pred_str === undefined) {
        pred_str = "true";
    }

    pred = util.transform(pred_str, table.schema);
    if (table !== undefined)
        return table.tuples.filter(pred);
    else
        throw "Table " + tbl_name + " not found!";
}

// Debug Functions

exports._is_loaded =  function() {
    return true;
}

exports._get_tables = function () {
    return tables;
}
