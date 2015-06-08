// table.js - the table class for ruddDB
var Schema = require("./schema");
// Solely required for make_schema
var types = require("./types");
var util = require("./util");
var Tuple = require("./tuple");
var concurrency = require("./concurrency");



// We should probably move this to schema.js, ie have the schema constructor take a variable
// (in terms of type) argument. Or maybe util?
// Makes a schema object out of an SQL style schema declaration.
function make_schema(s) {
    var names = [], types = [];

    s.split(",")
    // Get each table/type on its own
    .map(function(s) {
        return s.match(/\s*(\S)+\s+(\S+)\s*/).slice(1);
    })
    // Make the arrays to pass to schema
    .map(function (t) {
        names.push(t[0]);
        types.push(types.map_type(t[1]));
    });

    return new Schema(names, types);
}

module.exports = function Table (tbl_name, schema, keys) {
    if (typeof schema === "string")
        schema = make_schema(schema);

    if (keys == undefined) {
        keys = []
    }

    this.schema = schema;
    this.tbl_name = tbl_name;
    this.keys = keys;
    this.keysets_present = {}
    this.tuples = [];

    /* insert_tuple checks if tup matches the table's schema, then pushes
       the tuple into the table's tuple array if it does. */
    this.insert_tuple = function (tup, lock) {
        // If the tuple doesn't match the schema, it's an error.
        if (!schema.matches_tuple(tup)) {
            throw "Tuple doesn't match schema!";
        }

        /* If we have a non-trivial key, check what we're inserting vs the
           map of inserted key values */
        if (this.keys.length != 0) {
            key_project = util.create_project_function(this.keys, this.schema);
            tup_key = key_project(tup);
            if (this.keysets_present[tup_key]) {
                throw "Key error! A tuple with this key is already in the table."
            }
            else {
                this.keysets_present[tup_key] = true;
            }
        }
        
        if (!lock) {
            lock = new concurrency.Lock();
        }

        tup = new Tuple(tup, lock);
        tup.lock.old_values = null;
        lock.tuple = tup;
        console.log(lock);
        console.log(tup);
        this.tuples.push(tup);
    }

    /* delete_pred deletes tuples from the table which satisfy the predicate.
       If the predicate is empty, all tuples are deleted. */
    // TODO: Take care of keys when deleting
    this.delete_tuples = function (pred, txn_id) {
        this.tuples = this.tuples.filter(function (t) { return !pred(t.get_values(txn_id)); });
    }

    // TODO This doubles the time it takes to delete things. Any way we can
    // cache this info about which tuples are going to be deleted?
    this.filter_tuples = function (pred, txn_id) {
        return this.tuples.filter(function (t) {return t.get_values(txn_id) !== null
                                    && pred(t.get_values(txn_id));});
    }
        

    /* Updates tuples according to a mut(ation) and pred(icate) function.
       returns the number of tuples updated for logging */
    // TODO: Take care of keys when updating
    this.update_tuples = function(mut, pred, txn_id) {
        /* Transform the given functions, then just update each record
           one by one. */

        var tuple;
        var num_up = 0;
        for (var i = 0; i < this.tuples.length; i++) {
            tuple = this.tuples[i];
            if (pred(tuple.get_values(txn_id)))
                mut(tuple.get_values(txn_id));
                num_up++;
        }
        return num_up;
    }

	// Deletes all tuples with a null value. Used for finalizing concurrent deletes
    this.clear = function (txn_id) {
        this.tuples = this.tuples.filter(function (t) {return t.get_values(txn_id) != null});
    }

    // Returns schema, tbl_name, and tuples
    this.get_data = function() {
		throw "Not concurrent yet!"
        return {name   : this.tbl_name,
                schema : [this.schema.names, this.schema.types],
                tuples : this.tuples};
    }

    /* A helper for logging tuple deletion */
    this.num_tuples = function() {
//		throw "Not concurrent yet!"
//        return this.tuples.length;
    }
}
