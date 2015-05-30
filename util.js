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

function transform_pred(input, schema) {
    if(typeof input === "string") {
        var body = _transform_str(input, schema);
        eval("pred = function (tup) { return " + body + "}");
        return pred;
    } else {
        return _transform_func(input, schema);
    }
}

function transform_mut(input, schema) {
    if(typeof input === "string") {
        var body = _transform_str(input, schema);
        eval("mut = function (tup) { " + body + "; }");
        return mut;
    } else {
        return _transform_func(input, schema);
    }
}

function transform_fold(input, schema) {
    if(typeof input === "string") {
        var body = _transform_str(input, schema);

        // Now we have to look through the string and implement the accumulator
        var tuple_index = 0;
        var parens = 0;
        var new_body = '';
        var default_tuple = [];
        for(var i = 0; i < body.length; i++) {
            var c = body[i];
            if(c == '@')
                new_body += "acc[" + tuple_index + "]";
            else if(c == '{') {
                i++; // advance past the {
                var default_val = '';
                while(body[i] != '}') {
                    default_val += body[i];
                    i++;
                }
                default_tuple[tuple_index] = default_val;
            }
            else
                new_body += c;

            if (c == '(')
                parens++;
            else if(c == ')')
                parens--;
            else if(c == ',' && parens == 0)
                tuple_index++;
        }

        eval("func = function (acc, tup) { if(acc === undefined) { acc = [" +
            default_tuple + "]; } return [" + new_body + "]; }");
        return func;
    } else {
        return _transform_func(input, schema); // TODO should this get the acc treatment?
    }
}

function _transform_func(func, schema) {
    var func_str = func.toString();

    /* Find the relevant parts of the function string */
    var arg_start = func_str.indexOf("(") + 1;
    var arg_end = func_str.indexOf(")");
    var body_start = func_str.indexOf("{") + 1;
    var body_end = func_str.length - 1;

    /* Break the args out of the argument list, and get the corresponding
       indices of the column names in the table */
    var args = func_str.substring(arg_start, arg_end).split(",")
    args = args.map(function (x) {return x.trim();});
    if (args[0] == '') {
        return func;
    }
    var inds = args.map(function (x)
                           {return schema.get_index_of_col(x).toString();});

    /* Break the body of the function out, and take care of the relevant
       replacements, from column names to indexing into the tuple */
    var body = func_str.substring(body_start, body_end);
    for (var i = 0; i < args.length; i++) {
        var arg = args[i];
        var rep_string = "tup[" + inds[i] + "]";
        body = body.replace(new RegExp(arg, 'g'), rep_string);
    }

    /* Reconstruct the original function and return it. */
    eval("func = function (tup) {" + body + "}");
    return func;
}

/* transform takes a schema and a string, interprets the string as a function
   body, and replaces names with indices into the tuple. */
function _transform_str(str, schema) {

    // This is so the regex doesn't need to think about the beginning of the string
    str = " " + str + " ";

    for(var i = 0; i < schema.length; i++)
    {
        var name = schema.names[i];
        var regexp = new RegExp("(\\W)" + name + "(\\W)", "g");
        str = str.replace(regexp, "$1tup[" + i + "]$2");
    }

    return str;
}

/* Creates a function that projects a tuple based on an array of column names */
// TODO Can we use this to simplify the project node?
function create_project_function(cols, schema) {
    body = "function (tup) {return [";
    
    for (var i = 0; i < cols.length; i++) {
        var col = cols[i];
        body += "tup[" + schema.get_index_of_col(col) + "], ";
    }

    body = body.substring(0, body.length - 2);
    body += "];}"

    eval("var pred = " + body);
    return pred;
}

/* Creates a complete copy of a javascript object */
function deep_clone(source, dest) {
    if (source.length != undefined) {
        /* If length exists, it's an array. Copy over recursively. */
        dest = []
        for (var i = 0; i < source.length; i++) {
            if (typeof source[i] == "object") {
                deep_clone(source[i], dest[i]);
            }
            else {
                dest[i] = source[i];
            }
        }
    }
    else {
        /* It's an object. Copy all keys recursively. */
        source.keys().forEach(
            function (name) {
                if (typeof source[name] == "object") {
                    deep_clone(source[name], dest[name]);
                }
                else {
                    dest[name] = source[name];
                }})
    }
}

module.exports.array_eq = array_eq;
module.exports.array_deep_eq = array_deep_eq;
module.exports.zip = zip;
module.exports.transform_pred = transform_pred;
module.exports.transform_mut = transform_mut;
module.exports.transform_fold = transform_fold;
module.exports.create_project_function =  create_project_function;
