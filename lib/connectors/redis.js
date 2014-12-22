var redis = require('redis');
var async = require('async');
var _ = require('underscore');

var keys = require('haru-nodejs-util').keys;

var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;

var MAX_SLAVE_COUNTER = 100000000;

module.exports = Redis;

function Redis(config) {
    this.connection = [];

	this.connect(config);
};

inherits(Redis, EventEmitter);

Redis.prototype.connect = function(config) {
	var self = this;

    config.forEach(function(redisGroup) {
        var config = redisGroup.master;
        var group = {
            master: {},
            slaves: []
        };

        // master
        group.master = _addEventListener(redis.createClient(config.port, config.host), config, 'master');

        //slaves
        redisGroup.slaves.forEach(function(slave) {
            group.slaves.push( _addEventListener(redis.createClient(slave.port, slave.host), slave, 'slave' ) );
        });

        // add redis shard group
        self.connection.push(group);
    });
};

Redis.prototype.hget = function(key, field, callback, shardKey) {
	var self = this;

	var conn = self.getConnection('read', shardKey);

	conn.select(0);
	conn.hget(key, field, callback);
};

Redis.prototype.hset = function(key, field, value, callback, shardKey) {
    var self = this;

    var conn = self.getConnection('write', shardKey);

    conn.select(0);
    conn.hset(key, field, value, callback);
};

Redis.prototype.sadd = function(key, value, callback, shardKey) {
    var self = this;

    var conn = self.getConnection('write', shardKey);

    conn.select(0);
    conn.sadd(key, value, callback);
};

Redis.prototype.hvals = function( key, callback , shardKey) {
	var self = this;

	var conn = self.getConnection('read', shardKey);

	conn.select(0);
	conn.hvals(key, callback);
};

Redis.prototype.hgetall = function( key, callback , shardKey) {
	var self = this;

	var conn = self.getConnection('read', shardKey);

	conn.select(0);
	conn.hgetall(key, callback);
};

Redis.prototype.zrange = function( key, start, end, callback , shardKey) {
	var self = this;

	var conn = self.getConnection('read', shardKey);

	conn.select(0);
	conn.zrange(key, start, end, callback);
};

Redis.prototype.zincrby = function( key, increment, member, callback , shardKey) {
    var self = this;

    var conn = self.getConnection('write', shardKey);

    conn.select(0);
    conn.zincrby(key, increment, member, callback);
};

Redis.prototype.zrevrange = function( key, start, end, callback, shardKey ) {
    var self = this;

    var conn = self.getConnection('read', shardKey);

    conn.select(0);
    conn.zrevrange(key, start, end, callback);
};

Redis.prototype.zrem = function( key, member, callback, shardKey ) {
    var self = this;

    var conn = self.getConnection('write', shardKey);

    conn.select(0);
    conn.zrem(key, member, callback);
};

Redis.prototype.zadd = function(key, score, member, callback, shardKey) {
    var self = this;

    var conn = self.getConnection('write', shardKey);

    conn.select(0);
    conn.zadd(key, score, member, callback);
};

Redis.prototype.smembers = function( key, callback , shardKey) {
	var self = this;
	var conn = self.getConnection('read', shardKey);

	conn.select(0);

	conn.smembers(key, callback);	
};

Redis.prototype.hmset = function(key, feilds, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    callback = callback || function(error, results) {};

    conn.hmset( key, feilds , callback);
};

Redis.prototype.hmget = function(key, feilds, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('read', shardKey);

    conn.select(0);

    callback = callback || function(error, results) {};

    conn.hmget( key, feilds , callback);
};


Redis.prototype.hdel = function(key, field, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);


    conn.hdel( key, field , callback);
};

Redis.prototype.hmdel = function(key, fields, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    var multi = conn.multi();
    for(var i = 0; i < fields.length; i++) {
        multi.hdel(key, fields[i]);
    }

    multi.exec(callback);
};

Redis.prototype.hmsetnx = function(key, fields, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    var multi = conn.multi();

    var fields = _.pairs(fields);
    for( var i = 0; i < fields.length; i++ ) {
        multi.hsetnx(key, fields[i][0], fields[i][1] );
    }

    multi.exec(callback);
};

Redis.prototype.getConnection = function(method, shardKey) {
	var self = this;
    var conn;

    var shardNum = shardKey ? parseInt(shardKey) : 0;
    var index = 0;

    var redisGroup = self.connection[shardNum];

    if( !redisGroup ) {
        //TODO throw Error
        return ;
    }

    if( method === 'write' ) {
        conn = redisGroup.master;
    } else if(method === 'read') {
        conn = redisGroup.slaves[index];
    } else {
        //TODO Throw Error
    }

    return conn;
};

Redis.prototype.getShardCount = function() {
    return this.connection.length;
};

Redis.prototype.multi = function(shardKey) {
   var self = this;
   var conn = self.getConnection('write', shardKey);

   conn.select(0);

   return conn.multi();
};

Redis.prototype.ttl = function(key, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    conn.ttl(key, callback);
};

Redis.prototype.expire = function(key, seconds, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    conn.expire(key, seconds, callback);
};

Redis.prototype.del = function(key, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    conn.del(key, callback);
};

Redis.prototype.srem = function(key, members, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    if( !_.isArray(members) ) {
        members = [members];
    }

    conn.srem(key, members, callback);
};

Redis.prototype.sismember = function(key, member, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    conn.sismember(key, member, callback);
};


Redis.prototype.set = function(key, value, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('write', shardKey);

    conn.select(0);

    conn.set(key, value, callback);
};

Redis.prototype.get = function(key, callback, shardKey) {
    var self = this;
    var conn = self.getConnection('read', shardKey);

    conn.select(0);

    conn.get(key, callback);
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

