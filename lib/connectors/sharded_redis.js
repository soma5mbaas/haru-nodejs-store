
var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;

module.exports = Sharded_Redis;

function Sharded_Redis(config) {
    this.connection = [];
    this.connect(config);
};

inherits(Sharded_Redis, EventEmitter);

Sharded_Redis.prototype.connect = function(config) {
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


Sharded_Redis.prototype.getConnection = function(method, shardKey) {
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


function _getShardKey(entityId) {
    return entityId.substring(0, 1);
};


