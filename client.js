var http = require("http");
var rl = require("readline");
var url = process.argv[2] || "http://localhost:8090";

process.stdin.setEncoding('utf8');

var prompt = rl.createInterface({
    input: process.stdin,
    output: process.stdout
});

http.get(url, function(res) {
    if (res.statusCode === 200) {
        console.log("Connected to " + url + ".");
        prompt.question("> ", get_input);
    }
    else
        console.log("Got code " + res.statusCode + ".");
}).on('error', function(e) {
    console.log("Got error: " + e.message);
});

// Matches ["http"/"https", hostname, path, port].
var url_parser_regex = /(http|https):\/\/([^:\/]*)([^:]*)?:(\d*)/;
var parsed_url = url.match(url_parser_regex);

if (parsed_url === null) {
    console.log("Could not parse " + url + "!");
}

var post_options = {
    host: parsed_url[2],
    path: parsed_url[3] || "",
    port: parsed_url[4] || 80,
    method: 'POST',
    headers: {
        'Content-Type': "text/javascript",
        'Content-Length': 0
    }
};

function send_command(command) {
    post_options.headers["Content-Length"] = command.length;

    var post_req = http.request(post_options, function(res) {
        res.setEncoding("utf8");
        res.on("data", function (chunk) {
            if (chunk[chunk.length - 1] === "\n")
                prompt.write("Received:" + chunk);
            else
                prompt.write("Received: " + chunk + "\n");
        });
        res.on("end", function() {
            prompt.question("> ", get_input);
        });
    });

    post_req.write(command);
    post_req.end();
}

function get_input(str) {
    if (str === null)
        return;

    if (str === "quit"
        || str === "exit")
        process.exit();

    prompt.write("Sending: " + str + "\n");
    send_command(str);
    prompt.pause();
}