'use strict';

const mongoose = require('mongoose');

mongoose.Promise = global.Promise;

/**
 * Model类（操作数据表）
 * @param {string} name   数据表操作名
 * @param {object} schema 数据表结构
 * @param {object} conn   已打开数据库连接
 */
function Model(name, schema, conn){
	this.conn = conn;
	this.model = this.conn.model(name, new mongoose.Schema(schema));
}

Model.prototype = {
	/**
	 * 查询数据
	 * @param  {object}   query    查询条件，参考mongoose相关说明
	 * @param  {object}   options  查询附加选项，如page，sort等
	 * @param  {Function} callback 查询成功后的回调函数，第一个参数是error，第二个是查询结果
	 * @return {Query}             mongoose返回的对象
	 */
	get: function(query, options, callback){
		let Model = this.model;
		let Query = null;

		if( typeof(query) == 'function' ){
			callback = query;
			query = {};
		} else {
			query = query || {};
			options = options || {};
			callback = callback || function(){};
			if( typeof(options) == 'function' ){
				callback = options;
				options = {};
			}

			if( options.pageSize ){
				if( options.lastId ){
					query['_id'] = {'$gt': ObjectId(options.lastId)};
					Query = Model.find(query).limit(options.pageSize);
				} else {
					options.page = options.page || 1;
					Query = Model.find(query).skip(options.pageSize * (options.page - 1)).limit(options.pageSize);
				}
				options.sort = options.sort || -1;
			}
			if( options.sort ){
				Query = Query || Model.find(query);
				Query.sort({'_id': options.sort});
			}
		}

		Query = Query || Model.find(query);
		return Query.exec(callback);
	},

	/**
	 * 按表字段_id查询数据
	 * @param  {string}   id       查询的_id值
	 * @param  {Function} callback 查询成功后的回调函数，第一个参数是error，第二个是查询结果
	 * @return {Query}             mongoose返回的对象
	 */
	getById: function(id, callback){
		return this.get({_id: id}, callback);
	},

	/**
	 * 无条件查询
	 * @param  {object}   options  查询附加选项，如page，sort等
	 * @param  {Function} callback 查询成功后的回调函数，第一个参数是error，第二个是查询结果
	 * @return {Query}             mongoose返回的对象
	 */
	getAll: function(options, callback){
		callback = callback || function(){};
		if( typeof(options) == 'function' ){
			callback = options;
			options = {};
		}
		return this.get({}, options, callback);
	},

	/**
	 * 更新数据
	 * @param {object}   query    查询条件
	 * @param {object}   data     新的数据
	 * @param {Function} callback
	 */
	set: function(query, data, callback){
		let Model = this.model;

		callback = callback || function(){};

		return Model.find(query, (error, result) => {
			if (error) return callback(error, null);

			if (result && result.length > 0) {
				Model.update(query, {$set: data}, (updateErr) => {
					if (updateErr) return callback(updateErr, null);
					
					this.get({_id: result[0]._id}, callback);
				});
			} else {
				callback({message: 'not found'}, null);
			}
		});
	},

	/**
	 * 通过_id更新数据
	 * @param {string}   id       查询的_id值
	 * @param {object}   data     新的数据
	 * @param {Function} callback
	 */
	setById: function(id, data, callback){
		let Model = this.model;

		callback = callback || function(){};

		return Model.findByIdAndUpdate(id, {$set: data}, (error, result) => {
			if (error) return callback(error, null);
			
			this.get({_id: id}, callback);
		});
	},

	/**
	 * 更新数据，参照mongoose的方式更新
	 * @param {object}   query    查询条件
	 * @param {object}   update   更新数据的对象，参考mongoose相关说明
	 * @param {object}   options  更新数据的额外属性，参考mongoose相关说明
	 * @param {Function} callback
	 */
	update: function(query, update, options, callback){
		let Model = this.model;

		options = options || {};
		callback = callback || function(){};
		if( typeof(options) == 'function' ){
			callback = options;
			options = {};
		}

		return Model.find(query, (error, result) => {
			if (error) return callback(error, null);

			if (result && result.length > 0) {
				Model.update(query, update, options, (updateErr) => {
					if (updateErr) return callback(updateErr, null);
					
					this.get({_id: result[0]._id}, callback);
				});
			} else {
				callback({message: 'not found'}, null);
			}
		});
	},

	/**
	 * 通过_id更新数据，参照mongoose的方式更新
	 * @param {string}   id       查询的_id值
	 * @param {object}   update   更新数据的对象，参考mongoose相关说明
	 * @param {object}   options  更新数据的额外属性，参考mongoose相关说明
	 * @param {Function} callback
	 */
	updateById: function(id, update, options, callback){
		let Model = this.model;

		options = options || {};
		callback = callback || function(){};
		if( typeof(options) == 'function' ){
			callback = options;
			options = {};
		}

		return Model.findByIdAndUpdate(id, update, options, (error, result) => {
			if (error) return callback(error, null);
			
			this.get({_id: id}, callback);
		});
	},

	/**
	 * 删除数据
	 * @param {object}   query    查询条件
	 * @param {Function} callback
	 */
	delete: function(query, callback){
		let Model = this.model;

		callback = callback || function(){};

		return Model.find(query, (error, result) => {
			if (error) return callback(error, null);

			if (result && result.length > 0) {
				Model.remove(query, (deleteErr) => {
					if (deleteErr) return callback(deleteErr, null);
					callback(null, result);
				});
			} else {
				return callback({message: 'not found'}, null);
			}
		});
	},

	/**
	 * 通过_id删除数据
	 * @param {string}   id       查询的_id值
	 * @param {Function} callback
	 */
	deleteById: function(id, callback){
		let Model = this.model;

		callback = callback || function(){};

		return Model.findByIdAndRemove(id, (error, result) => {
			if (error) callback(error, null);
			callback(null, result);
		});
	},

	/**
	 * 创建新的记录
	 * @param  {object}   data     新的数据
	 * @param  {Function} callback 回调函数，第二个参数为新建的记录
	 * @return {Promise}           一个promise对象
	 */
	create: function(data, callback){
		let Model = this.model;

		callback = callback || function(){};

		return Model.create(data, (error, result) => {
			if (error) return callback(error, null);
			callback(null, result);
		});
	}
};


/**
 * 数据库连接类
 * @param {string} name 数据库操作名
 * @param {...}    args 打开数据库连接的参数
 */
function Connection(name, ...args){
	this.name = name;
	this.conn = mongoose.createConnection();
	this.connected = false;

	if( args.length > 0 ){
		this.open(...args);
	}
}

Connection.prototype = {
	/**
	 * 打开数据库连接
	 * @param  {...} args 打开数据库连接的参数
	 */
	open: function(...args){
		this.conn.open(...args);
		this.conn.once('connected', () => {
			this.connectUri = args[0];
			this.connected = true;
		});
	},

	/**
	 * 关闭数据库连接
	 * @param  {Function} callback 关闭后的回调函数
	 */
	close: function(callback){
		this.conn.close(() => {
			this.connected = false;
			callback && callback();
		});
	},

	/**
	 * 定义数据库可操作的表结构（打开数据表）
	 * @param {object} models 表结构的定义集合
	 */
	setModels: function(models){
		for( let name in models ){
			this[name] = new Model(name, models[name], this.conn);
		}
	}
};


module.exports = {
	_connections: {},

	createConnection: function(name, ...args){
		if( this._connections[name] ){
			let connection = this._connections[name];
			if( connection.connected == true ){
				connection.close(function(){
					connection.connected = false;
					connection.open(...args);
				});
			} else {
				connection.open(...args);
			}
		} else {
			this._connections[name] = new Connection(name, ...args);
			this[name] = this._connections[name];
		}
	},

	closeConnection: function(name, callback){
		if( name && this._connections[name] ){
			let connection = this._connections[name];
			connection.close(function(){
				connection.connected = false;
				callback && callback();
			});
		}
	}
};
