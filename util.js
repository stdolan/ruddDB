// util.js - Utility functions used throughout the codebase.

/*
 * array_compare takes two arrays, a and b, and recursively checks to see if
 * each element in a and b fits some compare function (which defaults to
 * type-strict equivalence).
 */
function array_compare(a, b, compare_func) {
    if (a.length != b.length)
        return false;

    compare_func = compare_func || function(x, y) { return x === y; };

    for (var i = 0; i < a.length; i++)
        if (a[i].constructor === Array) {
            if (!array_compare(a[i], b[i], compare_func))
                return false;
        }

        else if (!compare_func(a[i], b[i]))
            return false;

    return true;
}

/* zip combines a set of arrays element wise, and is equivalent to the standard
   zip function in lisp. */
function zip(arrays) {
    return arrays[0].map(function(_,i){
        return arrays.map(function(array){return array[i]})
    });
}

exports.array_compare = array_compare;
exports.zip = zip;
