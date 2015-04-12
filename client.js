var http = require("http");
var url = process.argv[2] || "http://localhost";

http.get(url, function(res) {
    console.log("Got response: " + res.statusCode);
}).on('error', function(e) {
    console.log("Got error: " + e.message);
});
