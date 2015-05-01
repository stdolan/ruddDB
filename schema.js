var types_ = require("./types");

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

    /* This function gets the index of a given column name in the schema */
    this.get_index_of_col = function (colname) {
        ind = this.names.indexOf(colname);
        if (ind == undefined) {
            throw "Column not found!";
        }
        return ind;
    }
	// TODO concat?
}
