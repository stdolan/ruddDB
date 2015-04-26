// schema.js - Implements schema for tables, and utility methods for them

var types_ = require("./types");
var util = require("./util");

module.exports = function Schema (names, types) {
	if(names.length !== types.length)
		throw "Number of names and types differ!";
	
	this.length = names.length;
	this.names = names;
    this.types = types;
	
	this.matches_tuple = function (tuple) {
		var t_len = tuple.length;
		if(t_len !== this.length)
			return false;
		
		for(var i = 0; i < t_len; i++)
			if(!types_.is_type(tuple[i], types[i]))
				return false;
			
		return true;
	}

	this.concat = function (other) {
		return new Schema (this.names.concat(other.names),
		                   this.types.concat(other.types));
	}

	this.equals = function(other) {
		return util.array_eq(names, other.names) &&
		        util.array_eq(types, other.types);
	}
}