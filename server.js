var twitter = require('twitter'),
    express = require('express'),
    app = express(),
    http = require('http'),
    server = http.createServer(app),
    io = require('socket.io').listen(server);

var bodyParser = require('body-parser');
app.use(bodyParser.json());

var shortid = require('shortid');


var twit = new twitter({
        consumer_key: 'xZdHxbEUtrK31U1jpwag4hV8T',
        consumer_secret: 'ynaCuQexW4jwew308TzYesa8aEfSHytQpgyU9iM4vCzeWHiTC2',
        access_token_key: '140491084-hi7UeXMLR4yvnzkljkQ3SQg1Mt7OkFAFoM1Sz1GD',
        access_token_secret: '9Y5j8w8lpS9ET0I5Cttf8L7Enoxdhnt4ko6zHllf9tpDV'
    }),
    stream = null;

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client();

var mysql = require("mysql");

// First you need to create a connection to the db
var con = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "twitter"
});

con.connect(function (err) {
    if (err) {
        console.log('Error connecting to Db');
        return;
    }
    console.log('Connection established');
});

app.use(express.static(__dirname + '/public'));
io.sockets.on('connection', function (socket) {

    socket.on("start tweets", function () {

        if (stream === null) {
            twit.stream('statuses/sample', function (stream) {
                stream.on('data', function (data) {
                    if (data != undefined) {
                        var tweet = new Object();
                        tweet.text = data.text;
                        if (data.source != undefined || data.source != null) {
                            var regex = /(<([^>]+)>)/ig;
                            tweet.source = data.source.replace(regex, "");
                            //console.log(tweet.source);
                        }

                        if (data.retweet_count != undefined || data.retweet_count != null) {
                            tweet.retweetcount = data.retweet_count;
                        }
                        if (data.created_at != undefined || data.created_at != null) {
                            tweet.time = Date.parse(data.created_at);
                        }

                        if (data.coordinates != undefined || data.coordinates != null) {
                            tweet.coordinates = data.coordinates.coordinates;
                            var cd = {
                                "lat": data.coordinates.coordinates[0],
                                "lng": data.coordinates.coordinates[1]
                            };
                            socket.broadcast.emit("twitter-stream", cd);
                            socket.emit('twitter-stream', cd);

                        }
                        if (data.user != undefined) {
                            if (data.user.screen_name != undefined || data.user.screen_name != null) {
                                tweet.username = data.user.screen_name;
                                //insertmysql(tweet.text,tweet.username);
                            }
                            if (data.user.location != undefined || data.user.location != null) {
                                tweet.userlocation = data.user.location;
                            }
                            if (data.user.lang != undefined || data.user.lang != null) {
                                tweet.lang = data.user.lang;
                            }
                        }
                        if (data.entities != undefined) {
                            if (data.entities.hashtags != undefined || data.entities.hashtags != null) {
                                if (data.entities.hashtags.length > 0) {
                                    tweet.hashtags = [];
                                    for (var i = 1; i < data.entities.hashtags.length; i++) {
                                        tweet.hashtags.push(data.entities.hashtags[i].text);
                                    }
                                }
                            }
                        }
                        if (data.place != undefined || data.place != null) {
                            if (data.place.full_name != undefined || data.place.full_name != null) {
                                tweet.tweetcity = data.place.full_name;
                            }
                            if (data.place.country != undefined || data.place.country != null) {
                                tweet.tweetcountry = data.place.country;
                            }
                            /*                    if (data.place.bounding_box != undefined || data.place.bounding_box != null) {
                                                    if (data.place.bounding_box.type != undefined || data.place.bounding_box.type != null) {
                                                        tweet.type = data.place.bounding_box.type;
                                                    }
                                                    if (data.place.bounding_box.coordinates != undefined || data.place.bounding_box.coordinates != null) {
                                                        tweet.tweetcoordinates = []
                                                        tweet.tweetcoordinates.push(data.place.bounding_box.coordinates[0]);
                                                        uploadDB(tweet);
                                                    }
                                                }*/
                        }
                        /*else {
                                           uploadDB2(tweet);
                                       }*/

                        uploadDB2(tweet); //elastic

                    }
                    stream.on('limit', function (limitMessage) {
                        return console.log(limitMessage);
                    });

                    stream.on('warning', function (warning) {
                        return console.log(warning);
                    });

                    stream.on('disconnect', function (disconnectMessage) {
                        return console.log(disconnectMessage);
                    });
                });
            });
        }


    });

    // Emits signal to the client telling them that the
    // they are connected and can start receiving Tweets
    socket.emit("connected");
});

function insertmysql(id, text) {
    var tweeps = {
        username: id,
        text: text
    };
    con.query('INSERT INTO tweets SET ?', tweeps, function (err, res) {
        if (err) throw err;
        console.log('Last insert ID:', res.insertId);
    });

}

function uploadDB(tweet) {
    //console.log(tweet);
    client.create({
        index: 'tweets',
        type: 'tweetjson',
        id: shortid.generate(),
        body: {
            content: tweet.text,
            source: tweet.source,
            tweetplace: tweet.tweetplace,
            retweetcount: tweet.retweetcount,
            time: tweet.time,
            //geo: tweet.geo,
            username: tweet.username,
            userlocation: tweet.userlocation,
            lang: tweet.lang,
            hashtags: tweet.hashtags,
            tweecity: tweet.tweetcity,
            tweetcountry: tweet.tweetcountry,
            coordinates: tweet.coordinates,
            "location": {
                "type": "polygon",
                "coordinates": tweet.tweetcoordinates
            }

        }
    }, function (error, response) {
        if (error) {
            console.log(error);
        }
        //console.log(response);
    });
}

function uploadDB2(tweet) {
    //console.log(tweet.text+'|'+tweet.username);
    client.create({
        index: 'tweetss',
        type: 'tweet',
        id: shortid.generate(),
        body: {
            content: tweet.text,
            source: tweet.source,
            tweetplace: tweet.tweetplace,
            retweetcount: tweet.retweetcount,
            time: tweet.time,
            //geo: tweet.geo,
            username: tweet.username,
            userlocation: tweet.userlocation,
            lang: tweet.lang,
            hashtags: tweet.hashtags,
            tweecity: tweet.tweetcity,
            tweetcountry: tweet.tweetcountry,
            coordinates: tweet.coordinates
        }
    }, function (error, response) {
        if (error) {
            console.log(error);
        }
        //console.log(response);
    });
}

server.listen(3000, function(){
    //Callback triggered when server is successfully listening. Hurray!
    console.log("Server listening on: http://localhost:3000");
});
