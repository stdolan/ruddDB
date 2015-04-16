// db.js - The main database object in ruddDB
var Table = require("./table")

var tables = {};

exports.create_table = function (tbl_name, schema) {
    tables.tbl_name = new Table(tbl_name, schema);
}
 
exports.insert = function (tbl_name, tup) {
    if (tables.tbl_name !== undefined)
        tables.tbl_name._insert_tuple(tup);
    else
        throw "Table " + tbl_name + " not found!";
}

exports.select = function (tbl_name, pred) {
    // If we didn't supply a predicate, return everything.
    if (pred === undefined) {
        pred = function (x) {return true;};
    }

    var table = tables.tbl_name;
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
