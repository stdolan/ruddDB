// nodes.js - Contains nodes, which form a pipeline of tuples to perform a query

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
		left.getSchema().concat(right.getSchema());
	}

	this.nextTuple = function () {
		// If the left child is empty
		if(currLeft === null)
			return null;
		
		var nextRight = right.nextTuple();
		if(nextRight === null) { // we exhausted the right
			right.reset();
			currLeft = left.nextTuple();
			if(currLeft === null)
				return null;
		}
		
		return currLeft.concat(nextRight);
	}
	
}

module.exports.TableNode = TableNode;
module.exports.SelectNode = SelectNode;
module.exports.JoinNode = JoinNode;