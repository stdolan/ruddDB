/* types.js - Provides a layer of types on top of Javascript's weak excuse for
types */

var INTEGER = 0;
var FLOAT = 1;
var STRING = 2;
var BOOLEAN = 3;

function is_type (val, type) {
	switch(type) {
		case INTEGER:
			return is_integer(val);
		case FLOAT:
			return (typeof val === 'number');
		case STRING:
			return (typeof val === 'string');
		case BOOLEAN:
			return (typeof val === 'boolean');
		default:
			throw "Unrecognized type! " + type;
	}
}

function is_integer(val) {
	if(typeof val === 'number') {
		// Jank as fuck, but it seems to work
		return (val | 0 === val);
	}
}

module.exports.INTEGER = INTEGER;
module.exports.FLOAT = FLOAT;
module.exports.STRING = STRING;
module.exports.BOOLEAN = BOOLEAN;

module.exports.is_type = is_type;
