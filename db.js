// db.js - The main database object in ruddDB
var Table = require("./table")

var tables = [];

exports.create_table = function (tbl_name, schema) {
    tables.push(new Table(tbl_name, schema));
}
 
exports.insert = function (tbl_name, tup) {
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        if (table.tbl_name === tbl_name) {
            table._insert_tuple(tup);
        }
    }
}

exports.select = function (tbl_name, pred) {
    // If we didn't supply a predicate, return everything.
    if (pred === undefined) {
        pred = function (x) {return true;};
    }

    for (var i = 0; i < tables.length; i++) {
        // Find the right table, and then return the appropriate tuples.
        var table = tables[i];
        if (table.tbl_name === tbl_name) {
            return table.tuples.filter(pred);
        }
    }
}

// Debug Functions

exports._is_loaded =  function() {
    return true;
}

exports._get_tables = function () {
    return tables;
}
