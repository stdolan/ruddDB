// nodes.js - Contains nodes, which form a pipeline of tuples to perform a query

var util = require('./util');

// TODO materialized node? just makes an array of tuples

/*
 * All nodes must have the following functions:
 * reset() - resets the stream of tuples to its start
 * get_schema() - returns the schema of the returned tuples
 * next_tuple() - returns the next tuple in the stream, or null if the stream
 *     is exhausted. If this is called multiple times after the stream is
 *     exhausted, it must not crash.
 * Additionally, they must be prepared upon creation, without an init method.
 */
function RenameNode(child, name) {
    this.reset = function() {
        child.reset();
    }
    
    this.get_schema = function () {
        return child.get_schema();
    }
    
    this.next_tuple = function () {
        return child.next_tuple();
    }
    
    this.get_name = function () {
        return name;
    }
}

function TableNode(t) {
    var table = t;
    var index = 0;

    this.reset = function () {
        index = 0;
    }

    this.get_schema = function () {
        return table.schema;
    }

    this.next_tuple = function () {
        if(index < table.tuples.length) {
            return table.tuples[index++];
        }
        else {
            return null;
        }
    }
    
    this.get_name = function () {
        return table.tbl_name;
    }
}

function SelectNode(child, pred, name) {

    // eta reduction, yo
    this.reset = child.reset;
    this.get_schema = child.get_schema;

    this.next_tuple = function () {
        while(true) {
            var tuple = child.next_tuple();

            if(tuple === null || pred(tuple.get_values())) {
                if (tuple) {
                    return tuple.get_values();
                }
                else {
                    return null;
                }
            }
        }
    }
    
    this.get_name = function () {
        return name;
    }
}

function JoinNode(left, right, name) {

    var left_sc = left.get_schema().copy();
    var right_sc = right.get_schema().copy();
    var l_name = left.get_name();
    var r_name = right.get_name();
    var intersection = left_sc.intersect(right_sc);
    left_sc.rename_cols(intersection, l_name);
    right_sc.rename_cols(intersection, r_name);
    var currLeft = left.next_tuple();
    var schema = left_sc.concat(right_sc);
    
    this.reset = function () {
        left.reset();
        right.reset();
        currleft = left.next_tuple();
    }

    this.get_schema = function () {
        return schema;
        
        /*
        return left.get_schema().concat(right.get_schema());*/
    }

    this.next_tuple = function () {
        // If the left child is empty
        if(currLeft === null)
            return null;

        var nextRight = right.next_tuple();
        if(nextRight === null) { // we exhausted the right
            right.reset();
            nextRight = right.next_tuple();
            currLeft = left.next_tuple();
            if(currLeft === null)
                return null;
        }
        return currLeft.get_values().concat(nextRight.get_values());
    }
    
    this.get_name = function () {
        return name;
    }
}

// project is a function that transforms tuples
function ProjectNode(child, schema, project, name) {

    this.reset = child.reset;

    this.get_schema = function () {
        return schema;
    }

    this.next_tuple = function () {
        var tup = child.next_tuple();
        if(tup === null)
            return null;
        var new_tup = project(tup.get_values());
        if (!schema.matches_tuple(new_tup)) {
            throw "Tuple doesn't match schema!";
        }
        return new_tup;
    }
    
    this.get_name = function () {
        return name;
    }
}

function UnionNode(left, right, name) {

    this.reset = function() {
        left.reset();
        right.reset();
    }

    this.get_schema = left.get_schema;

    this.next_tuple = function() {
        var tup = left.next_tuple();
        if(tup !== null)
            return tup.get_values();
        var right_next = right.next_tuple()
        if (right_next) {
            return right_next.get_values();
        }
        else {
            return null;
        }
    }
    
    this.get_name = function () {
        return name;
    }
    
    if(!left.get_schema().equals(right.get_schema()))
        throw "Schema do not match!";
}

// FoldingNode does an aggregation for functions that are 'fold-compatible';
// that is, associative and commutative. Examples: sum, count, min, max, but
// not average!
// group_func takes a tuple, and spits out something used for grouping. for
// most cases, this will be a subset of the columns. I suppose it could be used
// to create arbitrary equivalance classes though.
// fold_func takes an accumulated value (which may be undefined), and a tuple,
// and folds the tuple into the accumulator. You should be thinking "things you
// can pass foldl (for Haskell) or reduce (for Python).
function FoldingNode(child, group_func, fold_func, name) {

    var tuples_prepared = false;
    var index = 0;

    // TODO iterating through an array is O(n). idk how to hashmap in JS though :(
    var grouping_vars = [];
    var aggregate_vars = [];

    function prepare_tuples() {
        var tup = child.next_tuple();
        while(tup != null) {
            var group_tup = group_func(tup.get_values());
            var group_index = undefined;
            for(var i = 0; i < grouping_vars.length; i++)
                if(util.array_deep_eq(grouping_vars[i], group_tup))
                    group_index = i;

            if(group_index === undefined) {
                group_index = i; // it should be grouping_vars.length
                grouping_vars.push(group_tup);
                aggregate_vars.push(undefined);
            }

            aggregate_vars[group_index] = fold_func(aggregate_vars[group_index], tup.get_values());
            tup = child.next_tuple();
        }

        tuples_prepared = true;
    }

    this.reset = function() {
        child.reset();
        tuples_prepared = false;
    }

    this.get_schema = function() {
        // TODO
    }

    this.next_tuple = function() {
        if(!tuples_prepared)
            prepare_tuples()

        if(index == grouping_vars.length)
            return null;

        var tup = grouping_vars[index].concat(aggregate_vars[index]);
        index++;
        return tup;
        // TODO problem: gluing the tuples together will be tricky. what about
        // the other vars that aren't grouped by or aggregates?
    }
    
    this.get_name = function () {
        return name;
    }
}

module.exports.RenameNode = RenameNode;
module.exports.TableNode = TableNode;
module.exports.SelectNode = SelectNode;
module.exports.JoinNode = JoinNode;
module.exports.UnionNode = UnionNode;
module.exports.FoldingNode = FoldingNode;
module.exports.ProjectNode = ProjectNode;
