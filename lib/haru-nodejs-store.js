

module.exports = (function() {
    var inherits = require('util').inherits;
    var EventEmitter = require('events').EventEmitter;

    var Redis = require('./connectors').Redis;
    var MongoDb = require('./connectors').MongoDB;
    var MySql = require('./connectors').MySql;
    var Sharded_Redis = require('./connectors').Sharded_Redis;


    var _ = require('underscore');

    function Store() {
        this.connections = [];
    }

    inherits(Store, EventEmitter);

    Store.prototype.connect = function(configGroup) {
        if( !_.isObject(configGroup) ) { throw new Error('config must json object'); }
        var self = this;

        _.keys(configGroup).forEach(function(dbname) {
            var config = configGroup[dbname];
            var conn = {};

            config.namespace = configGroup.namespace;

            if(config.type === 'redis') {
                conn = new Redis(config.connections);
            } else if(config.type === 'mongodb') {
                conn = new MongoDb(config.connections);
            } else if(config.type === 'mysql') {
                conn = new MySql(config.connections);
            } else if( config.type === 'sharded_redis' ) {
                conn = new Sharded_Redis(config.connections);
            }

            if(conn) self.connections[dbname] = conn;
        });
    };

    Store.prototype.get = function(name) {
        return this.connections[name];
    };

    return new Store();
})();