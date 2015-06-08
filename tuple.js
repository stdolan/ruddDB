// tuple.js - The class for tuples in a table.

module.exports = function (values, lock) {
    this.values = values;
    this.lock = lock;

    this.get_values = function (txn_id) {
        if (this.lock.state == 1 && txn_id != this.lock.owner) {
            /* If the write lock is held, we should get the old values from the
               lock, assuming that we don't currently own the lock. */
            return this.lock.old_values;
        }
        else {
            return this.values;
        }
    }

    this.set_values_with_lock = function (values, txn_id) {
        /* Only allow the values to be set in this way if the txn is the current
           owner of a set lock. */
        if (this.lock.state == 1 && this.lock.owner == txn_id) {
            if(this.lock.old_values === undefined) {
                this.lock.old_values = this.values.slice();
            }
            this.values = values;
        }
        else {
            throw "Can't set values with lock if lock not owned!"
        }
    }
	
    this.mutate_with_lock = function (mut, txn_id) {
        /* Only allow the tuple to be mutated in this way if the txn is the current
           owner of a set lock. */
        if (this.lock.state == 1 && this.lock.owner == txn_id) {
            if(this.lock.old_values === undefined) {
                this.lock.old_values = this.values.slice();
            }
            mut(this.values);
        }
        else {
            throw "Can't set values with lock if lock not owned!"
        }
    }
}
