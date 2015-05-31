var http = require("http");
var port = 8090;
var db = require("./db");
var max_id = 0;

var server = http.createServer(function (req, res) {
    if (req.method === "GET") {
        var client_id = (max_id++).toString();
        res.writeHead(200, {
            'Content-Length': client_id.length,
            'Content-Type': 'text/plain'});
        res.end(client_id);
    }

    else if (req.method === "POST") {
        req.setEncoding("utf8");

        var command = "";

        req.on("readable", function() {
            command += req.read();
        });

        req.on('end', function () {
            var ret;
            try {
                ret = eval(command);
            }

            catch (e) {
                ret = "Error" + e.name + ": " + e.message;
                console.error(ret);

                res.writeHead(200, {
                    'Content-Length': ret.length,
                    'Content-Type': 'text/plain'
                });

                res.write(ret);
                res.end();
                return;
            }

            if (ret === undefined || ret === null) {
                res.writeHead(200, {
                    'Content-Length': 0,
                    'Content-Type': 'text/plain'
                });
                res.end();
            }
            
            else {
                ret = JSON.stringify(ret);

                res.writeHead(200, {
                    'Content-Length': ret.length,
                    'Content-Type': 'text/plain'
                });

                res.write(ret);
                res.end();
            }
        });
    }
});

server.listen(port);
