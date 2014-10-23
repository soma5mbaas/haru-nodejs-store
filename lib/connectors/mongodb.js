var mongodb = require('mongodb').MongoClient;
var DB = require('mongodb').Db;
var Server = require('mongodb').Server;
var async = require('async');

var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');



var DEFAULT_LIMIT = 50;
var MAX_LIMIT = 500;

module.exports = MongoDB;

function MongoDB(config) {
    this.connection = [];
    this.config = config;

	this.connect(config);
};

inherits(MongoDB, EventEmitter);

MongoDB.prototype.connect = function(config) {
	var self = this;

    async.timesSeries(config.length, function(n, next) {
        mongodb.connect(config[n].master.url, function(error, connection) {
            console.log('[%d] %s: MongoDB Connected', process.pid, config[n].master.url);
            next(error, connection);
        });
    }, function done(error, connections) {
        self.connection = connections;
        self.emit('connect', self);
    });
};

MongoDB.prototype.getConnection = function() {
    var self = this;
    var conn;

    var shardNum = 0;

    // mongodb 는 master만씀
    conn = self.connection[shardNum];

    return conn;
};

MongoDB.prototype.find = function(collection, condition, limit, callback) {
	var self = this;

    if( !callback && !limit ) {
        var coll = self.getConnection().collection( collection );
        return coll.find(condition);
    }

	process.nextTick(function() {
		var coll = self.getConnection().collection( collection );
		var _callback;
        var _limit = DEFAULT_LIMIT;

        condition = condition || {};

        if( _.isFunction(limit) ) {
            _callback = limit;
        } else if(_.isFunction(callback) ) {
            _limit = limit > MAX_LIMIT ? MAX_LIMIT : limit;
            _callback = callback;
        }

		coll.find(condition).limit(_limit).toArray(function(error, items) {
            _callback(error, items);
		});

	});
};


MongoDB.prototype.findCount = function(collection, condition, callback) {
    var self = this;

    process.nextTick(function() {
        var coll = self.getConnection().collection( collection );
        condition = condition || {};

        coll.find(condition).count(function(error, count) {
            callback(error, count);
        })

    });
};

MongoDB.prototype.findOne = function(collection, condition, callback) {
	var self = this;
	
	process.nextTick(function() {
		var coll = self.getConnection().collection( collection );
		condition = condition || {};

		coll.findOne( condition ,function(error, result) {
			return callback(error, result);
		});
	});
};

MongoDB.prototype.insert = function(collection, document, callback) {
    var self = this;
    process.nextTick(function() {
        var coll = self.getConnection().collection(collection);

        coll.insert(document, callback);
    });
};

MongoDB.prototype.update = function(collection, condition, document, options, callback) {
    var self = this;
    process.nextTick(function() {
        var coll = self.getConnection().collection(collection);
        var cb; // callback
        var op; // options

        if( _.isFunction(options) ) {
            cb = options;
            op = {};
        }
        if( _.isObject(options) ) {
            cb = callback;
            op = options;
        }

        coll.update(condition, document, op, cb);
    });
};

MongoDB.prototype.updateMany = function(collection, condition, document, callback) {
    var self = this;
    process.nextTick(function() {
        var coll = self.getConnection().collection(collection);

        coll.updateMany(condition, document, callback);
    });
};

MongoDB.prototype.remove = function(collection, condition, callback) {
    var self = this;
    process.nextTick(function() {
        var coll = self.getConnection().collection(collection);

        coll.remove(condition, callback);
    });
};

MongoDB.prototype.drop = function(collection, callback) {
    var self = this;
    process.nextTick(function() {
        var coll = self.getConnection().collection(collection);

        coll.drop(callback);
    });
};

MongoDB.prototype.addShardCollection = function(collection, callback) {
    if( !collection ) { return; }

    var self = this;
    var config = this.config[0].master;
    var db = new DB(config.db, new Server(config.host, config.port));
    db.open(function(error, db) {
       var admin = db.admin();

        admin.command({enablesharding: config.db}, function(error, results) {
            admin.command({shardcollection:config.db+'.'+collection, key:{_id:1} }, function(error, results) {
                console.log({shardcollection:config.db+'.'+collection, key:{_id:1} });
                db.close();
            });
        });
    });
};

function _addEventListener(conn, config, type){
    conn.on( 'connect', function() {
//        log.info('[%d] %s:%d Redis %s Connected', process.pid, config.host, config.port, type);
    }).on( 'error', function(error) {
//        log.error('[%d] %s:%d Redis %s Error : %s', process.pid, config.host, config.port, type, error.stack);
    }).on( 'close', function(hadError) {
//        log.error('[%d] %s:%d Redis %s Close', process.pid, config.host, config.port, type);
    });

    return conn;
};
