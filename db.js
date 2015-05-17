// db.js - The main database object in ruddDB
var Table = require("./table");
var Schema = require("./schema");
var nodes = require("./nodes");
var util = require("./util");
var fs = require("fs");

var tables = {};
var quiet = 0;


/* Creates a table. Equivalent to SQL: CREATE tbl_name (schema)
   schema is specified in SQL way, aka "a INTEGER, b STRING", etc. */
exports.create = function (tbl_name, schema) {
    tables[tbl_name] = new Table(tbl_name, schema);

    if (!quiet) {
        console.log("Created table " + tbl_name);
    }
}

/* Inserts rows into a table.
   Equivalent to SQL: INSERT INTO tbl_name VALUES tups */
exports.insert = function (tbl_name, tups) {

    var tbl = tables[tbl_name];
    if(tbl === undefined)
        throw "Table " + tbl_name + " not found!";

    for (var i = 0; i < tups.length; i++) {
        tables[tbl_name].insert_tuple(tups[i]);
    }

    /* If we're not being quiet, tell the user what happened */
    if (!quiet) {
        console.log("Inserted " + tups.length + " rows!");
    }
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

    /* Only get the number of rows if we need it to log, since it can be an
       expensive operation on very large tables */
    if (!quiet) {
        var pre_len = tables[tbl_name].num_tuples();
    }

    tables[tbl_name].delete_tuples(pred);

    /* If we're not being quiet, tell the user what happened */
    if (!quiet) {
        var post_len = tables[tbl_name].num_tuples();
        console.log("Deleted " + (pre_len - post_len) + " rows!");
    }
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
    var num_up = tbl.update_tuples(mut, pred);

    /* If we're not being quiet, tell the user what happened */
    if (!quiet) {
        console.log("Updated " + num_up + " rows!");
    }
    
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
    var num_ret = 0
    var tup = node.next_tuple();
    while(tup !== null) {
        ret.push(tup);
        tup = node.next_tuple();
        num_ret++;
    }

    /* If we're not being quiet, tell the user what happened */
    if (!quiet) {
        console.log("Selected " + num_ret + " rows!");
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
        pred = util.transform_pred(pred, resolve_table(child).get_schema());
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
exports.dump = function(filename) {
    filename = filename + ".dat";
    var data = {};
    data.tables = []
    for (table in tables)
        // See _get_data() for more on how the data is structured
        data.tables.push(tables[table].get_data());
    fs.writeFile(filename, JSON.stringify(data), function(err) {
        if (err) throw err;
        console.log("Database written to file " + filename);
    });
}

exports.load = function(filename) {
    filename = filename + ".dat";
    var data_str = fs.readFileSync(filename);
    var data = JSON.parse(data_str);
    
    /* Reconstruct each table */
    for (var i = 0; i < data.tables.length; i++) {
        table = data.tables[i];
        this.create(table.name, new Schema(table.schema[0], table.schema[1]));
        this.insert(table.name, table.tuples);
    }
}

exports.silence = function() {
    quiet = 1;
}

exports.unsilence = function() {
    quiet = 0;
}

// Debug Functions

exports._is_loaded =  function() {
    return true;
}

exports._get_tables = function () {
    return tables;
}
