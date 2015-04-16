var http = require("http");
var port = 8090;
var db = require("./db");

var server = http.createServer(function (req, res) {
    res.writeHead(200, {
        'Content-Length': 0,
        'Content-Type': 'text/plain' });
    res.end();
});

server.listen(port);
