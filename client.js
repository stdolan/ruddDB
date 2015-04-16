var http = require("http");
var os = require("os");
var url = process.argv[2] || "http://localhost:8090";

process.stdin.setEncoding('utf8');

http.get(url, function(res) {
    if (res.statusCode === 200) {
        console.log("Connected to " + url + ".");
        repl();
    }
    else
        console.log("Got code " + res.statusCode + ".");
}).on('error', function(e) {
    console.log("Got error: " + e.message);
});

function repl() {
    process.stdout.write("> ");

    process.stdin.on('readable', function() {
        var chunk = process.stdin.read();

        if (chunk === null)
            return;

        if (chunk === "quit" + os.EOL
            || chunk === "exit" + os.EOL
            || chunk === "q" + os.EOL)
            process.exit();

        process.stdout.write('data: ' + chunk);
        process.stdout.write("> ");
    });
}