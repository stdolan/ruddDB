// nodes.js - Contains nodes, which form a pipeline of tuples to perform a query

var util = require('./util');

/*
 * All nodes must have the following functions:
 * reset() - resets the stream of tuples to its start
 * getSchema() - returns the schema of the returned tuples
 * nextTuple() - returns the next tuple in the stream, or null if the stream
 *     is exhausted. If this is called multiple times after the stream is
 *     exhausted, it must not crash.
 * Additionally, they must be prepared upon creation, without an init method.
 */     

function TableNode(t) {
	var table = t;
    var index = 0;
	
	this.reset = function () {
		index = 0;
	}
	
	this.getSchema = function () {
		return table.schema;
	}
	
	this.nextTuple = function () {
		if(index < table.tuples.length)
			return table.tuples[index++];
		else
			return null;
	}
}

function SelectNode(child, pred) {

	// eta reduction, yo
	this.reset = child.reset;
	this.getSchema = child.getSchema;
	
	this.nextTuple = function () {
		while(true) {
			var tuple = child.nextTuple();
			if(tuple === null || pred(tuple))
				return tuple;
		}
	}
}

function JoinNode(left, right) {
	
	var currLeft = left.nextTuple();
	
	this.reset = function () {
		left.reset();
		right.reset();
		currLeft = left.nextTuple();
	}
	
	this.getSchema = function () {
		return left.getSchema().concat(right.getSchema());
	}

	this.nextTuple = function () {
		// If the left child is empty
		if(currLeft === null)
			return null;
		
		var nextRight = right.nextTuple();
		if(nextRight === null) { // we exhausted the right
			right.reset();
			nextRight = right.nextTuple();
			currLeft = left.nextTuple();
			if(currLeft === null)
				return null;
		}
		
		return currLeft.concat(nextRight);
	}
}

function ProjectNode(child, schemaMap) {

	this.reset = child.reset;

	this.getSchema = function () {
        // TODO
	}

	this.nextTuple = function () {
		// TODO
	}
}

function UnionNode(left, right) {

    this.reset = function() {
		left.reset();
		right.reset();
	}
	
	this.getSchema = left.getSchema;

	this.nextTuple = function() {
	    var tup = left.nextTuple();
		if(tup !== null)
			return tup;
		return right.nextTuple();
	}
	
	if(!left.getSchema().equals(right.getSchema()))
		throw "Schema do not match!";
}

module.exports.TableNode = TableNode;
module.exports.SelectNode = SelectNode;
module.exports.JoinNode = JoinNode;
module.exports.UnionNode = UnionNode;