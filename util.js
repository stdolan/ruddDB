// util.js - Utility functions used throughout the codebase.

/*
 * array_eq takes two arrays, a and b, and recursively checks to see if
 * each element in a and b fits some compare function (which defaults to
 * type-strict equivalence).
 */
function array_eq(a, b, eq_func) {
	if(a.length !== b.length)
		return false;
	
	eq_func = eq_func || function(x, y) { return x === y; };
	
	for (var i = 0; i < a.length; i++)
		if(!eq_func(a[i], b[i]))
			return false;

	return true;
}

function array_deep_eq(a, b, eq_func) {
	if(a.length !== b.length)
		return false;
	
	eq_func = eq_func || function(x, y) { return x === y; };
	
	for (var i = 0; i < a.length; i++) {
		if(a[i].constructor === Array) {
			if(!array_deep_eq(a[i], b[i], eq_func))
				return false;
		}
			
		else if(!eq_func(a[i], b[i]))
			return false;
	}

	return true;
}

/* zip combines a set of arrays element wise, and is equivalent to the standard
   zip function in lisp. */
function zip(arrays) {
    return arrays[0].map(function(_,i){
        return arrays.map(function(array){return array[i]})
    });
}

/* transform takes a schema and a string, interprets the string as a function
   body, transforms it to a function that can be directly applied to tuples in
   the table. */
function transform(func_str, schema) {
	
	// This is so the regex doesn't need to think about the beginning of the string
	func_str = " " + func_str + " ";
	
	for(var i = 0; i < schema.length; i++)
	{
		var name = schema.names[i];
		var regexp = new RegExp("(\\W)" + name + "(\\W)", "g");
		func_str = func_str.replace(regexp, "$1tup[" + i + "]$2");
	}
		
    /* Reconstruct the original function and return it. */
    eval("pred = function (tup) { return " + func_str + "; }");
    return pred;
}

module.exports.array_eq = array_eq;
module.exports.array_deep_eq = array_deep_eq;
module.exports.zip = zip;
module.exports.transform = transform;