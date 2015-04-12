var tables = [];

exports._is_loaded =  function() {
    return true;
}

exports._get_tables = function () {return tables;};

/* Table functionality */
function Table (tbl_name, schema) {
    this.schema = schema;
    this.tbl_name = tbl_name;
    this.tuples = [];
    
    this._insert_tuple = function (tup) {
        if (tup.map(function(x) {return typeof x;}).join(",") != schema.join(",")) {
            throw "Tuple doesn't match schema!";
        }
        this.tuples.push(tup);
    }
}

exports.create_table = function (tbl_name, schema) {
    tables.push(new Table(tbl_name, schema));
}

exports.insert = function (tbl_name, tup) {
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        if (table.tbl_name === tbl_name) {
            table._insert_tuple(tup);
        }
    }
}

exports.select = function (tbl_name, pred) {
    if (pred === undefined) {
        pred = function (x) {return true;};
    }
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        if (table.tbl_name === tbl_name) {
            return table.tuples.filter(pred);
        }
    }
}
