// tuple.js - The class for tuples in a table.

module.exports = function (values, lock) {
    this.values = values;
    this.lock = lock;

    this.get_values = function (txn_id) {
        if (this.lock.state == 1 && txn_id != lock.owner) {
            /* If the write lock is held, we should get the old values from the
               lock, assuming that we don't currently own the lock. */
            return this.lock.old_values;
        }
        else {
            return this.values;
        }
    }
}
