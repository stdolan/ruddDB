var http = require("http");
var port = 80;

var server = http.createServer(function (req, res) {
	var msg = 'Pong!';
	res.writeHead(200, {
		'Content-Length': msg.length,
  		'Content-Type': 'text/plain' });
	res.end(msg);
});

server.listen(port);