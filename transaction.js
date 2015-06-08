// transaction.js - the transaction handler for ruddDB
var clone = require("clone");

var Table = require("./table");
var util = require("./util");
var nodes = require("./nodes");
var concurrency = require("./concurrency");
var Tuple = require ("./tuple");

module.exports = function Transaction (table, type, next_id) {
    
    /* Set up the transaction depending on the type */
    switch(type) {
        case "copy":
            this.type = "copy";
            this.orig = table;
            this.table = clone(table);
            break;

        case "lock":
            this.type = "lock";
            this.table = table;
            this.id = next_id;
            this.locks = [];
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
                var row_lock = new concurrency.Lock();
                row_lock.tuple = tups[i];
                func_queue.enqueue(this.table.insert_tuple, [tups[i], row_lock, true],
                                   row_lock, this.id, this.table);
                this.locks.push(row_lock);
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
            var to_delete = this.table.filter_tuples(pred, this.id);
            var delete_func = function (tup) {
                tup.set_values_with_lock(null, this.id);
            }
                
            for (var i=0; i < to_delete.length; i++) {
                del_tup = to_delete[i];
                func_queue.enqueue(delete_func, [del_tup], del_tup.lock, this.id, this);
            }
            
//            this.table.clear(this.id);
            
            for (var i = 0; i < to_delete.length; i++) {
                this.locks.push(to_delete[i].lock);
            }
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
        
        mut = util.transform_mut(mut, this.table.schema);
        var num_up;
        if (this.type == "copy") {
            num_up = this.table.update_tuples(mut, pred);
        }
        else {
            num_up = 0;
            var to_update = this.table.filter_tuples(pred, this.id);
            var update_func = function (tup) {
                num_up++;
                tup.mutate_with_lock(mut, this.id);
            }
            /* Queue each value for updating */
            for (var i = 0; i < to_update.length; i++) {
                up_tup = to_update[i]
                func_queue.enqueue(update_func, [up_tup], up_tup.lock, this.id, this);
            }
            
            /* Push locks to be freed. */
            for (var i = 0; i < to_update.length; i++) {
                this.locks.push(to_update[i].lock);
            } 
        }

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
        
        return new nodes.SelectNode(new nodes.TableNode(this.table), pred, null, this.id);
    }

    this.rollback = function () {
        /* Kick out any locks that we didn't end up getting */
        txn_id = this.id;
        this.locks = this.locks.filter(function (lock) {return lock.owner == txn_id});

        if(type == "copy") {
            delete(this.table);
        }
        else {
            for (var i = 0; i < this.locks.length; i++) {
                //revert to original values
                if (this.locks[i].old_values !== null) {
                    this.locks[i].tuple.values = this.locks[i].old_values.slice();
                }
            }
            this.table.clear(this.id);
            for (var i = 0; i < this.locks.length; i++) {
                this.locks[i].free();
            }
            this.locks = [];
        }
    }
    
    this.commit = function () {
        switch(type) {
            case "copy":
                /* Replace the original table with the updated table. */
                this.orig.tuples = this.table.tuples;
                break;
            case "lock":
                this.table.clear(this.id);
                for (var i = 0; i < this.locks.length; i++) {
                    this.locks[i].free();
                }
                this.locks = [];
                break;
        }
    }
}
