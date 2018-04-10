'use strict';

const mongoose = require('mongoose');

mongoose.Promise = global.Promise;

/**
 * Model类（操作数据表）
 * @param {string} name   数据表操作名
 * @param {object} schema 数据表结构
 * @param {object} conn   已打开数据库连接
 */
function ModelClass(name, schema, conn) {
	let Schema;
	if (schema.hasOwnProperty('_id')) {
		Schema = new mongoose.Schema(schema, {_id: false});
	} else {
		Schema = new mongoose.Schema(schema);
	}
	this.name = name;
	this.conn = conn;
	this.Schema = Schema;
	this.__schema = schema;
	this.model = this.conn.model(name, Schema, name);
}

ModelClass.prototype = {
	schemaAdd: function(schema) {
		this.Schema.add(schema);
	},

	schemaReset: function() {
		let Schema;
		if (this.__schema.hasOwnProperty('_id')) {
			Schema = new mongoose.Schema(this.__schema, {_id: false});
		} else {
			Schema = new mongoose.Schema(this.__schema);
		}
		this.Schema = Schema;
		this.model = this.conn.model(this.name, Schema, this.name);
	},

	query: function(query) {
		query = query || {};

		return this.model.find(query);
	},

	/**
	 * 查询数据
	 * @param  {object}   query    查询条件，参考mongoose相关说明
	 * @param  {object}   options  查询附加选项，如page，sort等
	 * @param  {Function} callback 查询成功后的回调函数，第一个参数是error，第二个是查询结果
	 * @return {Query}             mongoose返回的对象
	 */
	get: function(query, options, callback) {
		const Model = this.model;
		let Query = null;

		if (typeof (query) == 'function') {
			callback = query;
			query = {};
		} else {
			query = query || {};
			options = options || {};
			callback = callback || function() {};
			if (typeof (options) == 'function') {
				callback = options;
				options = {};
			}

			if (options.pageSize) {
				if (options.lastId) {
					query['_id'] = {$gt: mongoose.Schema.Types.ObjectId(options.lastId)};
					Query = Model.find(query).limit(options.pageSize);
				} else {
					options.page = options.page || 1;
					Query = Model.find(query)
								.skip(options.pageSize * (options.page - 1))
								.limit(options.pageSize);
				}
				options.sort = options.sort || -1;
			}
			if (options.sort) {
				Query = Query || Model.find(query);
				const sort =  (Object.prototype.toString.call(options.sort) === '[object Object]') ? options.sort : {_id: options.sort};
				Query.sort(sort);
			}
		}

		Query = Query || Model.find(query);
		return Query.exec(callback);
	},

	/**
	 * 无条件查询
	 * @param  {object}   options  查询附加选项，如page，sort等
	 * @param  {Function} callback 查询成功后的回调函数，第一个参数是error，第二个是查询结果
	 * @return {Query}             mongoose返回的对象
	 */
	getAll: function(options, callback) {
		callback = callback || function() {};
		if (typeof (options) == 'function') {
			callback = options;
			options = {};
		}

		return this.get({}, options, callback);
	},

	/**
	 * 按表字段_id查询数据
	 * @param  {string}   id       查询的_id值
	 * @param  {Function} callback 查询成功后的回调函数，第一个参数是error，第二个是查询结果
	 * @return {Query}             mongoose返回的对象
	 */
	getById: function(id, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.findById(id, function(error, result) {
			if (error) {
				callback(error, null);
				return;
			}
			callback(null, result);
		});
	},

	getOne: function(query, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.findOne(query, function(error, result) {
			if (error) {
				callback(error, null);
				return;
			}
			callback(null, result);
		});
	},

	/**
	 * 更新数据
	 * @param {object}   query    查询条件
	 * @param {object}   data     新的数据
	 * @param {Function} callback
	 */
	set: function(query, data, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.find(query, (error, result) => {
			if (error) {
				callback(error, null);
				return;
			}
			if (result && result.length > 0) {
				Model.update(query, {$set: data}, {multi: true}, (updateErr) => {
					if (updateErr) {
						callback(updateErr, null);
						return;
					}
					this.get({_id: result[0]._id}, callback);
				});
				return;
			}
			callback({message: 'not found'}, null);
		});
	},

	/**
	 * 通过_id更新数据
	 * @param {string}   id       查询的_id值
	 * @param {object}   data     新的数据
	 * @param {Function} callback
	 */
	setById: function(id, data, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.findByIdAndUpdate(id, {$set: data}, (error, result) => {
			if (error) {
				callback(error, null);
				return;
			}
			this.getById(id, callback);
		});
	},

	/**
	 * 更新数据，参照mongoose的方式更新
	 * @param {object}   query    查询条件
	 * @param {object}   update   更新数据的对象，参考mongoose相关说明
	 * @param {object}   options  更新命令的参数
	 * @param {Function} callback
	 */
	update: function(query, update, options, callback) {
		const Model = this.model;

		if (typeof (options) == 'function') {
			callback = options;
			options = {};
		} else {
			options = options || {};
			callback = callback || function() {};
		}

		return Model.update(query, update, options, (error) => {
			if (error) {
				callback(error, null);
				return;
			}
			this.get(query, callback);
		});
	},

	/**
	 * 通过_id更新数据，参照mongoose的方式更新
	 * @param {string}   id       查询的_id值
	 * @param {object}   update   更新数据的对象，参考mongoose相关说明
	 * @param {Function} callback
	 */
	updateById: function(id, update, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.findByIdAndUpdate(id, update, (error, result) => {
			if (error) {
				callback(error, null);
				return;
			}
			this.getById(id, callback);
		});
	},

	/**
	 * 删除数据
	 * @param {object}   query    查询条件
	 * @param {Function} callback
	 */
	delete: function(query, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.find(query, (error, result) => {
			if (error) {
				callback(error, null);
				return;
			}
			if (result && result.length > 0) {
				Model.remove(query, (deleteErr) => {
					if (deleteErr) {
						callback(deleteErr, null);
						return;
					}
					callback(null, result);
				});
				return;
			}
			callback({message: 'not found'}, null);
		});
	},

	/**
	 * 通过_id删除数据
	 * @param {string}   id       查询的_id值
	 * @param {Function} callback
	 */
	deleteById: function(id, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.findByIdAndRemove(id, (error, result) => {
			if (error) {
				callback(error, null);
				return;
			}
			callback(null, result);
		});
	},

	/**
	 * 创建新的记录
	 * @param  {object}   data     新的数据
	 * @param  {Function} callback 回调函数，第二个参数为新建的记录
	 * @return {Promise}           一个promise对象
	 */
	create: function(data, callback) {
		const Model = this.model;

		callback = callback || function() {};

		return Model.create(data, (error, result) => {
			if (error) {
				callback(error, null);
				return;
			}
			callback(null, result);
		});
	}
};


/**
 * 数据库连接类
 * @param {string} name 数据库操作名
 * @param {...}    args 打开数据库连接的参数
 */
function Connection(name, ...args) {
	this.name = name;
	this.connected = false;

	this.conn = mongoose.createConnection();

	if (args.length > 0) {
		this.open(...args);
	}
}

Connection.prototype = {
	/**
	 * 格式化打开数据库连接的参数
	 * @param  {...} args 打开数据库连接的参数
	 */
	formatUri: function(...args) {
		const dbServer = args[0], dbName = args[1];
		let dbOptions, callback;

		dbOptions = args[2];
		callback = args[3];

		if (typeof (dbOptions) == 'function') {
			callback = dbOptions;
			dbOptions = {};
		} else {
			if (Object.prototype.toString.call(dbOptions) !== '[object Object]') {
				dbOptions = {};
			}
			if (typeof (callback) != 'function') {
				callback = function() {};
			}
		}

		if (dbOptions.user && dbOptions.pass) {
			this.connectUri = 'mongodb://' + dbOptions.user + ':' + dbOptions.pass + '@' + dbServer + ':' + (dbOptions.port || 27017);
		} else {
			this.connectUri = 'mongodb://' + dbServer + ':' + (dbOptions.port || 27017);
		}
		delete dbOptions.user;
		delete dbOptions.pass;
		delete dbOptions.port;
		dbOptions.useMongoClient = true;

		this.connectUri += '/' + dbName;
		this.connectOptions = dbOptions;

		return callback;		
	},

	/**
	 * 打开数据库连接
	 * @param  {...} args 打开数据库连接的参数
	 */
	open: function(...args) {
		const callback = this.formatUri(...args);
		this.conn.openUri(this.connectUri, this.connectOptions, callback);
		this.conn.once('connected', () => {
			this.connected = true;
		});
	},

	/**
	 * 关闭数据库连接
	 * @param  {Function} callback 关闭后的回调函数
	 */
	close: function(callback) {
		this.conn.close(() => {
			this.connected = false;
			callback && callback();
		});
	},

	/**
	 * 定义数据库可操作的表结构（打开数据表）
	 * @param {object} models 表结构的定义集合
	 */
	setModels: function(models) {
		for (const name in models) {
			this[name] = new ModelClass(name, models[name], this.conn);
		}
	}

	/**
	 * 定义数据库可操作的表结构（打开数据表）
	 * @param {object} models 表结构的定义集合
	 */
	setModel: function(modelName, schema) {
		const model = new ModelClass(modelName, schema, this.conn);
		this[modelName] = model;
		return model;
	}
};


module.exports = {
	_connections: {},

	connectDatebase: function(connectedName, ...args) {
		if (this._connections[connectedName]) {
			const connection = this._connections[connectedName];
			if (connection.connected == true) {
				connection.close(function() {
					connection.connected = false;
					connection.open(...args);
				});
			} else {
				connection.open(...args);
			}
		} else {
			this[connectedName] = new Connection(connectedName, ...args);
			this._connections[connectedName] = this[connectedName];
		}

		return this[connectedName];
	},

	closeDatabase: function(connectedName, callback) {
		if (!this._connections[connectedName]) {
			return;
		}
		const connection = this._connections[connectedName];
		connection.close(function() {
			connection.connected = false;
			callback && callback();
		});
	}
};
