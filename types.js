// types.js - Provides a layer of types on top of Javascript's weak excuse for types

var INTEGER = 0;
var FLOAT = 1;
var STRING = 2;
var BOOLEAN = 3;

function is_type (val, type) {
	switch(type) {
		case INTEGER:
		    // use isSafeInteger? or else we leak into float territory
			return Number.isInteger(val)
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

function matches_schema (tuple, schema) {
	var t_len = tuple.length;
	var s_len = schema.length;
	if(t_len != s_len)
		return false;
	
	for(var i = 0; i < t_len; i++)
		if(!is_type(tuple[i], schema[i]))
			return false;
		
	return true;
}

module.exports.INTEGER = INTEGER;
module.exports.FLOAT = FLOAT;
module.exports.STRING = STRING;
module.exports.BOOLEAN = BOOLEAN;

module.exports.is_type = is_type;
module.exports.matches_schema = matches_schema;