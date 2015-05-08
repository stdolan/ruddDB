/* types.js - Provides a layer of types on top of Javascript's weak excuse for
types */

var INTEGER = 0;
var FLOAT = 1;
var STRING = 2;
var BOOLEAN = 3;

// Mapping between string and types.
var mapping = {
	"INTEGER": 0,
	"FLOAT": 1,
	"STRING": 2,
	"BOOLEAN": 3
};

// Given a string, returns the number corresponding to that type, undefined
// if it is not a recognized type.
module.exports.map_type = function(s) {
	return mapping[s.toUpperCase()];
}

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
	return false;
}

module.exports.INTEGER = INTEGER;
module.exports.FLOAT = FLOAT;
module.exports.STRING = STRING;
module.exports.BOOLEAN = BOOLEAN;

module.exports.is_type = is_type;
