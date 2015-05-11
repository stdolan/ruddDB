// db.js - The main database object in ruddDB
var Table = require("./table");
var Nodes = require("./nodes");
var util = require("./util");

var tables = {};


/* Creates a table. Equivalent to SQL: CREATE tbl_name (schema)
   schema is specified in SQL way, aka "a INTEGER, b STRING", etc. */
exports.create = function (tbl_name, schema) {
    tables[tbl_name] = new Table(tbl_name, schema);
}

/* Inserts a row into a table.
   Equivalent to SQL: INSERT INTO tbl_name VALUE tup */
exports.insert = function (tbl_name, tup) {
    if (tables[tbl_name] !== undefined) {
        tables[tbl_name].insert_tuple(tup);
    }
    else {
        throw "Table " + tbl_name + " not found!";
    }
}

/* Deletes tuples from a table that satisfy the given predicate
   Equivalent to SQL: DELETE FROM tbl_name WHERE pred */
exports.delete = function (tbl_name, pred) {

    // If we didn't supply a predicate, delete everything.
    if (pred === undefined) {
        pred = function () {return true;};
    }

    if (tables[tbl_name] !== undefined) {
	    tables[tbl_name].delete_tuples(pred);
    }
	else {
	    throw "Table " + tbl_name + " not found!";
    }
}

/* Updates tuples in a table according to the given function, in all rows
   which satisfy the given predicate.
   Equivalent to SQL: UPDATE tbl_name SET mut WHERE pred */
exports.update = function (tbl_name, mut, pred) {
    if (tables[tbl_name] !== undefined) {
	    tables[tbl_name].update_tuples(mut, pred);
    }
	else {
	    throw "Table " + tbl_name + " not found!";
    }
}

/* Selects rows from a table.
   Equivalent to SQL: SELECT * FROM tbl_name WHERE pred */
exports.select = function (tbl_name, pred) {
    var table = tables[tbl_name];

    // If we didn't supply a predicate, return everything.
    if (pred === undefined) {
        pred = function () {return true;};
    }

    if (table !== undefined) {
        var node = new Nodes.SelectNode(new Nodes.TableNode(table), pred);
        var ret = [];
        var curr = node.next_tuple();
        while (curr !== null) {
            ret.push(curr);
            curr = node.next_tuple();
        }
        return ret;
    }
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

exports.join = function(left_tbl_name, right_tbl_name, join_type) {
    if (!join_type || join_type === "cross") {
        var node = new Nodes.JoinNode(new Nodes.TableNode(tables[left_tbl_name]),
            new Nodes.TableNode(tables[right_tbl_name]));
        var curr = node.nextTuple();
        if (curr === null)
            return null;
        var ret = [];
        while (curr !== null) {
            ret.push(curr);
            curr = node.nextTuple();
        }
        return ret;
    }
}
