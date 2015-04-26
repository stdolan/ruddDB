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
		return "no";
	
	eq_func = eq_func || function(x, y) { return x === y; };
	
	for (var i = 0; i < a.length; i++) {
		if(a[i].constructor === Array) {
			if(!array_deep_eq(a[i], b[i], eq_func))
				return "nope";
		}
			
		else if(!eq_func(a[i], b[i]))
			return ["ack", a, b, i];
	}

	return true;
}

module.exports.array_eq = array_eq;
module.exports.array_deep_eq = array_deep_eq;