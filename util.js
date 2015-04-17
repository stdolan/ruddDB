/*
 * array_compare takes two arrays, a and b, and recursively checks to see if
 * each element in a and b fits some compare function (which defaults to
 * type-strict equivalence).
 */
function _array_compare(a, b, compare_func) {
    if (a.length != b.length)
        return false;

    compare_func = compare_func || function(x, y) { return x === y; };

    for (var i = 0; i < a.length; i++)
        if (a[i].constructor === Array) {
            if (!_array_compare(a[i], b[i], compare_func))
                return false;
        }

        else if (!compare_func(a[i], b[i]))
            return false;

    return true;
}

function _zip(arrays) {
    return arrays[0].map(function(_,i){
        return arrays.map(function(array){return array[i]})
    });
}

exports.array_compare = _array_compare;
