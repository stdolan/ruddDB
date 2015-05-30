// transaction.js - the transaction handler for ruddDB
var clone = require("clone");

var Table = require("./table");
var util = require("./util");
var nodes = require("./nodes");

var next_id = 1;
var txns = [];

function Transaction () {

    this.id = next_id;
	next_id++;
	
	this.tables = {};
}

function start_transaction() {
	var txn = new Transaction();
	txns.push(txn);
	return txn.id;
}

function get_txn_idx(id) {
	for(var i = 0; i < txns.length; i++)
		if(txns[i].id === id)
			return i;
		
	return undefined;
}

function valid_txn(id) {
	return get_txn_idx(id) !== undefined;
}

function rollback(id) {
	var i = get_txn_idx(id);
	
	if(i === undefined)
		throw "Transaction " + id + " is no longer active!"

	// Remove the transaction from the list
	txns.splice(i, 1);
}

function commit(tables, id) {
	var i = get_txn_idx(id);
	
	if(i === undefined)
		throw "Transaction " + id + " is no longer active!"
	
    var txn = txns[i];
	for(var name in txn.tables) {
		tables[name] = txn.tables[name];
	}
	
	// All other transactions are invalid now!
	// TODO just remove the ones that intersect with the committed transaction
	// TODO also, what should the following sequence do? Right now, B commits
	//     start(A), start(B), commit(B), commit(A)
	txns = [];
}

function get_clone(table, id) {
	var i = get_txn_idx(id);
	
	if(i === undefined)
		throw "Transaction " + id + " is no longer active!"
	
	var txn = txns[i];
	var t = txn.tables[table.tbl_name];
    if(t === undefined) {
        t = clone(table);
        txn.tables[table.tbl_name] = t;
    }
	
    return t;
}

module.exports.start_transaction = start_transaction;
module.exports.valid_txn = valid_txn;
module.exports.rollback = rollback;
module.exports.commit = commit;
module.exports.get_clone = get_clone;