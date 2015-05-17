// db.js - The main database object in ruddDB
var Table = require("./table");
var nodes = require("./nodes");
var util = require("./util");
var fs = require("fs");

var tables = {};


/* Creates a table. Equivalent to SQL: CREATE tbl_name (schema)
   schema is specified in SQL way, aka "a INTEGER, b STRING", etc. */
exports.create = function (tbl_name, schema) {
    tables[tbl_name] = new Table(tbl_name, schema);
}

/* Inserts rows into a table.
   Equivalent to SQL: INSERT INTO tbl_name VALUES tup */
exports.insert = function (tbl_name, tups) {

    var tbl = tables[tbl_name];
    if(tbl === undefined)
        throw "Table " + tbl_name + " not found!";

    for (var i = 0; i < tups.length; i++) {
        tables[tbl_name].insert_tuple(tups[i]);
    }
    console.log("Inserted " + tups.length + " rows!");
}

/* Deletes tuples from a table that satisfy the given predicate
   Equivalent to SQL: DELETE FROM tbl_name WHERE pred */
exports.delete = function (tbl_name, pred) {

    var tbl = tables[tbl_name];
    if(tbl === undefined)
        throw "Table " + tbl_name + " not found!";

    // If we didn't supply a predicate, delete everything.
    if (pred === undefined) {
        pred = function () {return true;};
    } else {
        pred = util.transform_pred(pred, tbl.schema);
    }

    tables[tbl_name].delete_tuples(pred);
}

/* Updates tuples in a table according to the given function, in all rows
   which satisfy the given predicate.
   Equivalent to SQL: UPDATE tbl_name SET mut WHERE pred */
exports.update = function (tbl_name, mut, pred) {

    var tbl = tables[tbl_name];
    if(tbl === undefined)
        throw "Table " + tbl_name + " not found!";

    if (pred === undefined) {
        pred = function (tup) {return true;};
    } else {
        pred = util.transform_pred(pred, tbl.schema);
    }

    mut = util.transform_mut(mut, tbl.schema);

    tbl.update_tuples(mut, pred);
}

/* Helper function. If passed a table name, returns a table node, and if passed
   a node, just returns it. */
function resolve_table(arg) {
    var table = tables[arg];
    if(table !== undefined)
        return new nodes.TableNode(table);

    // TODO make sure it's a node!

    return arg;
}

/* Given a tree of nodes, returns the resulting tables. */
exports.eval = function (node) {
    var ret = [];
    var tup = node.next_tuple();
    while(tup !== null) {
        ret.push(tup);
        tup = node.next_tuple();
    }

    return ret;
}

/* Selects rows from a table.
   Equivalent to SQL: SELECT * FROM child WHERE pred */
exports.select = function (child, pred) {

    // If we didn't supply a predicate, return everything.
    if (pred === undefined) {
        pred = function () {return true;};
    } else {
        pred = util.transform_pred(pred, schema);
    }

    return new nodes.SelectNode(resolve_table(child), pred);
}

exports.join = function(left_child, right_child, join_type) {

    if(!join_type || join_type === "cross")
        return new nodes.JoinNode(resolve_table(left_child), resolve_table(right_child));

    throw "Join type \"" + join_type + "\" is unsupported";
}

// TODO making the user specify the schema is gross. can we do better?
exports.project = function(child, schema, project) {

    child = resolve_table(child);
    project = util.transform_mut(project, child.get_schema());

    return new nodes.ProjectNode(child, schema, project);
}

exports.union = function(left_child, right_child) {
    return new nodes.UnionNode(resolve_table(left_child), resolve_table(right_child));
}

exports.fold = function(child, group, fold) {

    child = resolve_table(child);

    group = util.transform_pred(group, child.get_schema());
    fold = util.transform_fold(fold, child.get_schema());

    return new nodes.FoldingNode(child, group, fold);
}

// Writes the entirety of the tables to the file, sans Table class functions.
exports.dump = function() {
    var filename = "db_" + Date.now() + ".dat";
    var data = [];
    for (table in tables)
        // See _get_data() for more on how the data is structured
        data.push(tables[table]._get_data());
    fs.writeFile(filename, data, function(err) {
        if (err) throw err;
        return "Database written to file " + filename + ".\n";
    });
}

// Debug Functions

exports._is_loaded =  function() {
    return true;
}

exports._get_tables = function () {
    return tables;
}