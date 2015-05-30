// transaction.js - the transaction handler for ruddDB
var clone = require("clone");

var Table = require("./table");
var util = require("./util");
var nodes = require("./nodes");

module.exports = function Transaction (table, type) {
    
    /* Set up the transaction depending on the type */
    switch(type) {
        case "copy":
            this.type = "copy";
            this.orig = table;
            this.table = clone(table);
            break;

        case "lock":
            throw "Lock transactions not yet implemented!";
            break;

        default:
            throw "Unknown transaction type: " + type;
    }

    this.insert = function (tbl_name, tups) {

        if (tbl_name != this.table.tbl_name) {
            throw "Table " + tbl_name + " isn't part of this transaction!";
        }

        for (var i = 0; i < tups.length; i++) {
            if (this.type == "copy") {
                this.table.insert_tuple(tups[i]);
            }
            else {
                /* TODO Lock the rows/table? We have to decide what to do
                   in these cases */
            }
        }

        /* If we're not being quiet, tell the user what happened */
        if (!quiet) {
            console.log("Inserted " + tups.length + " rows!");
        }
    }

    this.delete = function (tbl_name, pred) {

        if (tbl_name != this.table.tbl_name) {
            throw "Table " + tbl_name + " isn't part of this transaction!";
        }

        // If we didn't supply a predicate, delete everything.
        if (pred === undefined) {
            pred = function () {return true;};
        } else {
            pred = util.transform_pred(pred, this.table.schema);
        }

        /* Only get the number of rows if we need it to log, since it can be an
           expensive operation on very large tables */
        if (!quiet) {
            var pre_len = this.table.num_tuples();
        }

        if (this.type == "copy") {
            this.table.delete_tuples(pred);
        }
        else {
            /* TODO Lock the rows/table? We have to decide what to do
               in these cases */
        }

        /* If we're not being quiet, tell the user what happened */
        if (!quiet) {
            var post_len = this.table.num_tuples();
            console.log("Deleted " + (pre_len - post_len) + " rows!");
        }
    }

    this.update = function (tbl_name, mut, pred) {

        if (tbl_name != this.table.tbl_name) {
            throw "Table " + tbl_name + " isn't part of this transaction!";
        }

        if (pred === undefined) {
            pred = function (tup) {return true;};
        } else {
            pred = util.transform_pred(pred, this.table.schema);
        }

        if (this.type == "copy") {
            mut = util.transform_mut(mut, this.table.schema);
        }
        else {
            /* TODO Lock the rows/table? We have to decide what to do
               in these cases */
        }




        var num_up = this.table.update_tuples(mut, pred);

        /* If we're not being quiet, tell the user what happened */
        if (!quiet) {
            console.log("Updated " + num_up + " rows!");
        }
        
    }

    this.select = function (tbl_name, pred) {

        if (tbl_name != this.table.tbl_name) {
            throw "Table " + tbl_name + " isn't part of this transaction!";
        }

        // If we didn't supply a predicate, return everything.
        if (pred === undefined) {
            pred = function () {return true;};
        } else {
            pred = util.transform_pred(pred, this.table.schema);
        }

        return new nodes.SelectNode(new nodes.TableNode(this.table), pred);
    }

    this.commit = function () {
        switch(type) {
            case "copy":
                /* Replace the original table with the updated table. */
                this.orig.tuples = this.table.tuples;
                break;
            case "lock":
                /* TODO Free all the locks? */
                break;
        }
    }

    this.rollback = function() {
        switch(type) {
            case "copy":
                this.table.tuples = clone(this.orig.tuples);
                break;

            case "lock":
                // TODO free all locks?
                break;
        }
    }
}
