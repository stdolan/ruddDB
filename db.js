module.exports = function() {
    var db = {};
    var tables = [];
    
    db.is_loaded = function() {
        return true;
    }

    return db;
}