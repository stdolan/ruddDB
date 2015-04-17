var http = require("http");
var port = 8090;
var db = require("./db");

var server = http.createServer(function (req, res) {
    if (req.method === "GET") {
        res.writeHead(200, {
            'Content-Length': 0,
            'Content-Type': 'text/plain'});
        res.end();
    }

    else if (req.method === "POST") {
        req.setEncoding("utf8");

        var command = "";

        req.on("readable", function() {
            command += req.read();
        });

        req.on('end', function () {
            var ret = JSON.stringify(eval(command));

            res.writeHead(200, {
                'Content-Length': ret.length,
                'Content-Type': 'text/plain'
            });

            res.write(ret);
            res.end();
        });
    }
});

server.listen(port);
