

function TableNode(t) {
	var table = t;
    var index = 0;
	
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
	
	// TODO can this be eta-reduced?
	this.getSchema = function () {
		return child.getSchema();
	}
	
	this.nextTuple = function () {
		while(true) {
			var tuple = child.nextTuple();
			if(tuple === null || pred(tuple))
				return tuple;
		}
	}
}

module.exports.TableNode = TableNode;
module.exports.SelectNode = SelectNode;