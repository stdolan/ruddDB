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

function transform_pred(pred, schema) {
    var pred_str = pred.toString();

    /* Find the relevant parts of the function string */
    var arg_start = pred_str.indexOf("(") + 1;
    var arg_end = pred_str.indexOf(")");
    var body_start = pred_str.indexOf("{") + 1;
    var body_end = pred_str.length - 1;

    /* Break the args out of the argument list, and get the corresponding
       indices of the column names in the table */
    var args = pred_str.substring(arg_start, arg_end).split(",")
    args = args.map(function (x) {return x.trim();});
    if (args[0] == '') {
        return pred;
    }
    var inds = args.map(function (x)
                           {return schema.get_index_of_col(x).toString();});

    /* Break the body of the function out, and take care of the relevant
       replacements, from column names to indexing into the tuple */
    var body = pred_str.substring(body_start, body_end);
    for (var i = 0; i < args.length; i++) {
        var arg = args[i];
        var rep_string = "tup[" + inds[i] + "]";
        body = body.replace(arg, rep_string);
    }

    /* Reconstruct the original function and return it. */
    eval("pred = function (tup) {" + body + "}");
    return pred;
}

/* transform takes a schema and a string, interprets the string as a function
   body, transforms it to a function that can be directly applied to tuples in
   the table. */
function transform_pred_str(func_str, schema) {
	
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

function transform_mut_str(func_str, schema) {
	
	// This is so the regex doesn't need to think about the beginning of the string
	func_str = " " + func_str + " ";
	
	for(var i = 0; i < schema.length; i++)
	{
		var name = schema.names[i];
		var regexp = new RegExp("(\\W)" + name + "(\\W)", "g");
		func_str = func_str.replace(regexp, "$1tup[" + i + "]$2");
	}
		
    /* Reconstruct the original function and return it. */
    eval("pred = function (tup) { " + func_str + "; }");
    return pred;
}

module.exports.array_eq = array_eq;
module.exports.array_deep_eq = array_deep_eq;
module.exports.zip = zip;
module.exports.transform_pred_str = transform_pred_str;
module.exports.transform_mut_str = transform_mut_str;
module.exports.transform_pred = transform_pred;
