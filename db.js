// db.js - The main database object in ruddDB
var Table = require("./table")

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

/* Selects rows from a table.
   Equivalent to SQL: SELECT * FROM tbl_name WHERE pred */
exports.select = function (tbl_name, pred) {
    var table = tables.tbl_name;

    // If we didn't supply a predicate, return everything.
    if (pred === undefined) {
        pred = function (x) {return true;};
    }
    else {
        pred = table.transform_pred(pred);
    }
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
