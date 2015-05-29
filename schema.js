// schema.js - Implements schema for tables, and utility methods for them

var types_ = require("./types");
var util = require("./util");

module.exports = function Schema (names, types) {
	if(names.length !== types.length)
		throw "Number of names and types differ!";
	
	this.length = names.length;
	this.names = names;
    this.types = types;
	
    /* matches_tuple checks if an input tuple matches the schema in terms
       of typing. */
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
    
    /* Creates a clone of the schema for manipulation */
    this.copy = function() {
        return new Schema (names.slice(), types.slice());
    }
    
    /* Receives a schema as an argument and returns an array of
       column names that appear in both schemas */
    this.intersect = function(other) {
        var copy = other.names.slice();
//        console.log(copy);
        for(var i = copy.length - 1; i >= 0 ; i--) {
            var ind = names.indexOf(copy[i]);
            if(ind < 0 || types[ind] !== other.types[ind])
                copy.splice(i, 1);
        }
//        console.log(copy);
        return copy;
    }
    
    /* takes as arguments a list of column names to change and the
       prefix representing the table name.  Changes a column name s
       to "prefix.s". */
    this.rename_cols = function (inter, prefix) {
        for(var i = 0; i < inter.length; i++) {
            var ind = names.indexOf(inter[i]);
            names[ind] = prefix + "." + names[ind];
        }
    }
    
	this.equals = function(other) {
		return util.array_eq(names, other.names) &&
		        util.array_eq(types, other.types);
	}

    /* This function gets the index of a given column name in the schema */
    this.get_index_of_col = function (colname) {
        ind = this.names.indexOf(colname);
        if (ind == undefined) {
            throw "Column not found!";
        }
        return ind;
    }
}
