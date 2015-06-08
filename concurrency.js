// concurrency.js  - The concurrency primitives for ruddDB

/* Lock class */

exports.Lock = function () {
    this.state = 0;
    this.owner = null;
    this.old_values = undefined;
    this.tuple = undefined;

    this.free = function () {
        this.state = 0;
        this.owner = null;
        this.old_values = undefined;
    }
    
}

/* Want Nodes for use in determining deadlock in the function queue. */

WantNode = function () {
    this.id = undefined;
    this.children = [];
    this.parents = []
}

/* Function Queue */

exports.FunctionQueue = function () {

    this.queue = []
    this.fails = 0
    this.want_nodes = {}

    this.enqueue = function (func, args, lock, txn_id, container) {
        this.queue.push([func, args, lock, txn_id, container]);
        this.fails = 0;
        this.run();
    }

    this.exec_proc = function () {
        proc = this.queue.shift();
        var lock = proc[2];
        var txn_id = proc[3];
        if (lock.state == 0) {
            lock.state = 1;
            lock.owner = txn_id;
            /* If we have a container specified, we should use that to apply
               the function (important for evaluating table functions under
               tables.) */
            proc[0].apply(proc[4] || proc[0], proc[1]);
        }
        else {
            var wanted_lock = proc[2];
            var wanter_id = proc[3];
            var wanter_node;
            var owner_node;
            this.queue.push(proc);
            this.fails += 1;

            /* Get the nodes corresponding to the wanter and owner of the lock. */
            if (this.want_nodes[wanter_id] == undefined) {
               wanter_node = new WantNode();
               wanter_node.id =  wanter_id;
                this.want_nodes[wanter_id] = wanter_node;
            }
            else {
                wanter_node = this.want_nodes[wanter_id]
            }

            if (this.want_nodes[wanted_lock.owner] == undefined) {
                owner_node = new WantNode();
                owner_node.id = wanted_lock.owner;
                this.want_nodes[wanted_lock.owner] = owner_node;
            }
            else {
                owner_node = this.want_nodes[wanted_lock.owner];
            }

            /* If the corresponding edge isn't already in the graph, add it! */
            if (wanter_node.children.indexOf(owner_node) == -1) {
                wanter_node.children.push(owner_node);
                owner_node.parents.push(wanter_node);
            }

            return;
        }

        this.fails = 0;
    }

    /* NOTE: This cycle seeking is O(n^2). I wrote it at 5 am. Please rewrite
       if anyone ever cares about this project. */

    this.seek_cycle_help = function (txn_id, node) {

        if (node.id == txn_id) {
            /* There's a cycle! */
            return true;
        }
        else if (node.children.length == 0) {
            return false;
        }
        else {
            /* Recursively call the function on all the children. */
            for (var i = 0; i < node.children.length; i++) {
                if (this.seek_cycle_help(txn_id, node.children[i])) {
                    return true;
                }
            }
        }
    }

    this.seek_cycle = function (txn_id) {
        /* Get the node corresponding to this txn_id, and see if we can detect
           a cycle that includes it. */
        var node = this.want_nodes[txn_id];

        /* Call the helper on each child to search for cycles */
        for (var i = 0; i < node.children.length; i++) {
            if (this.seek_cycle_help(txn_id, node.children[i])) {
                return true;
            }
        }

        return false;
    }

    this.check_deadlock = function () {
        var found_deadlock = false;
        var wanter_list = Object.keys(this.want_nodes);

        for (var i = 0; i < wanter_list.length; i++) {
            if (this.seek_cycle(wanter_list[i]) && wanter_list[i] != 0) {
                /* Ack! Wanter i is causing deadlock. Let's kick him out, as long
                   as he's not the main session. */
                this.queue = this.queue.filter(
                    function (proc) {
                        return proc[3] != wanter_list[i];
                     }
                );

                var locking_node = this.want_nodes[wanter_list[i]];
                
                for (var j = 0; j < locking_node.parents.length; j++) {
                    var parent = locking_node.parents[j];
                    var index_of_locking = parent.children.indexOf(locking_node);
                    parent.children.splice(index_of_locking, 1);
                }

                delete this.want_nodes[wanter_list[i]];

                transaction_map[wanter_list[i]].rollback();

                if (!quiet) {
                    console.log("Aborted transaction " + wanter_list[i] + " due to deadlock");
                }

                found_deadlock = true;
                break;
            }
        }

        if (found_deadlock) {
            this.fails = 0;
            this.run();
        }
    }

    this.run = function () {
        while (this.queue.length != 0 && this.queue.length > this.fails) {
            this.exec_proc();
        }
        /* Check for deadlock if the queue isn't empty! */
        if (this.queue.length != 0) {
            this.check_deadlock();
        }
    }
}
