'use strict';

var platform      = require('./platform'),
	async         = require('async'),
	moment        = require('moment'),
	isNil         = require('lodash.isnil'),
	isEmpty       = require('lodash.isempty'),
	isArray       = require('lodash.isarray'),
	isNumber      = require('lodash.isnumber'),
	contains      = require('lodash.contains'),
	isString      = require('lodash.isstring'),
	isPlainObject = require('lodash.isplainobject'),
	cassandra, schema, Data;

let insertData = function (processedData, callback) {
	var data = new Data(processedData);

	data.save(callback);
};

let processData = function (data, callback) {
	let fields        = schema.fields,
		processedData = {};

	async.forEachOf(fields, (field, key, done) => {
		try {
			let dataType = '';

			if (isString(fields[key]))
				dataType = `${fields[key]}`.toLowerCase();
			else if (isPlainObject(fields[key]) && fields[key].type)
				dataType = `${fields[key].type}`.toLowerCase();

			// bigint
			if (!isNil(fields[key]) && (dataType === 'bigint' || dataType === 'bigint') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.Long.fromString(`${data[key]}`);

			// blob
			else if (!isNil(fields[key]) && (dataType === 'blob' || dataType === 'blob') && Buffer.isBuffer(data[key]))
				processedData[key] = data[key];
			else if (!isNil(fields[key]) && (dataType === 'blob' || dataType === 'blob') && !isNil(data[key]))
				processedData[key] = new Buffer(`${data[key]}`, 'base64');

			// boolean
			else if (!isNil(fields[key]) && (dataType === 'boolean' || dataType === 'boolean'))
				processedData[key] = (data[key] || !isNil(data[key])) ? true : false;

			// counter
			else if (!isNil(fields[key]) && (dataType === 'counter' || dataType === 'counter') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.Long.fromString(`${data[key]}`);

			// date
			else if (!isNil(fields[key]) && (dataType === 'date' || dataType === 'date') && Buffer.isBuffer(data[key]))
				processedData[key] = cassandra.datatypes.LocalDate.fromBuffer(moment(`${data[key]}`).toDate());
			else if (!isNil(fields[key]) && (dataType === 'date' || dataType === 'date') && !isNil(data[key]) && moment(`${data[key]}`).isValid())
				processedData[key] = cassandra.datatypes.LocalDate.fromDate(moment(`${data[key]}`).toDate());
			else if (!isNil(fields[key]) && (dataType === 'date' || dataType === 'date') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.LocalDate.fromString(`${data[key]}`);

			// decimal
			else if (!isNil(fields[key]) && (dataType === 'decimal' || dataType === 'decimal') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.BigDecimal.fromString(`${data[key]}`);

			// double, float, int, smallint, tinyint
			else if (!isNil(fields[key]) && (contains(['double', 'float', 'int', 'smallint', 'tinyint'], dataType) || contains(['double', 'float', 'int', 'smallint', 'tinyint'], dataType)) && !isNil(data[key]))
				processedData[key] = Number(`${data[key]}`);

			//inet
			else if (!isNil(fields[key]) && (dataType === 'inet' || dataType === 'inet') && Buffer.isBuffer(data[key]))
				processedData[key] = new cassandra.datatypes.InetAddress(data[key]);
			else if (!isNil(fields[key]) && (dataType === 'inet' || dataType === 'inet') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.InetAddress.fromString(`${data[key]}`);

			// list, set
			else if (!isNil(fields[key]) && (contains(['list', 'set'], dataType) || contains(['list', 'set'], dataType)) && isArray(data[key]))
				processedData[key] = data[key];
			else if (!isNil(fields[key]) && (contains(['list', 'set'], dataType) || contains(['list', 'set'], dataType)) && !isNil(data[key]))
				processedData[key] = JSON.parse(`${data[key]}`);

			// map
			else if (!isNil(fields[key]) && (dataType === 'map' || dataType === 'map') && isPlainObject(data[key]))
				processedData[key] = data[key];
			else if (!isNil(fields[key]) && (dataType === 'map' || dataType === 'map') && !isNil(data[key]))
				processedData[key] = JSON.parse(`${data[key]}`);

			// time
			else if (!isNil(fields[key]) && (dataType === 'time' || dataType === 'time') && Buffer.isBuffer(data[key]))
				processedData[key] = cassandra.datatypes.LocalTime.fromBuffer(moment(`${data[key]}`).toDate());
			else if (!isNil(fields[key]) && (dataType === 'time' || dataType === 'time') && isNumber(data[key]))
				processedData[key] = cassandra.datatypes.LocalTime.fromMilliseconds(data[key]);
			else if (!isNil(fields[key]) && (dataType === 'time' || dataType === 'time') && !isNil(data[key]) && moment(`${data[key]}`).isValid())
				processedData[key] = cassandra.datatypes.LocalTime.fromDate(moment(`${data[key]}`).toDate());
			else if (!isNil(fields[key]) && (dataType === 'time' || dataType === 'time') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.LocalTime.fromString(`${data[key]}`);

			// timestamp
			else if (!isNil(fields[key]) && (dataType === 'timestamp' || dataType === 'timestamp') && !isNil(data[key]) && moment(`${data[key]}`).isValid())
				processedData[key] = moment(`${data[key]}`).toDate();

			// timeuuid
			else if (!isNil(fields[key]) && (dataType === 'timeuuid' || dataType === 'timeuuid') && !isNil(data[key]) && moment(`${data[key]}`).isValid())
				processedData[key] = cassandra.datatypes.TimeUuid.fromDate(moment(`${data[key]}`).toDate());
			else if (!isNil(fields[key]) && (dataType === 'timeuuid' || dataType === 'timeuuid') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.TimeUuid.fromString(`${data[key]}`);

			// tuple
			else if (!isNil(fields[key]) && (dataType === 'tuple' || dataType === 'tuple') && isArray(data[key]))
				processedData[key] = cassandra.datatypes.Tuple.fromArray(data[key]);

			// uuid
			else if (!isNil(fields[key]) && (dataType === 'uuid' || dataType === 'uuid') && Buffer.isBuffer(data[key]))
				processedData[key] = new cassandra.datatypes.Uuid(data[key]);
			else if (!isNil(fields[key]) && (dataType === 'uuid' || dataType === 'uuid') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.Uuid.fromString(`${data[key]}`);

			// varint
			else if (!isNil(fields[key]) && (dataType === 'varint' || dataType === 'varint') && Buffer.isBuffer(data[key]))
				processedData[key] = cassandra.datatypes.Integer.fromBuffer(moment(`${data[key]}`).toDate());
			else if (!isNil(fields[key]) && (dataType === 'varint' || dataType === 'varint') && !isNil(data[key]))
				processedData[key] = cassandra.datatypes.Integer.fromString(`${data[key]}`);

			// ascii, text, varchar and others
			else
				processedData[key] = (!isNil(data[key])) ? `${data[key]}` : undefined;

			done();
		}
		catch (ex) {
			done();
		}
	}, () => {
		callback(null, processedData);
	});
};

platform.on('data', function (data) {
	if (isPlainObject(data)) {
		processData(data, (error, processedData) => {
			insertData(processedData, (error) => {
				if (!error) {
					platform.log(JSON.stringify({
						title: 'Record Successfully inserted to Cassandra Database.',
						data: processedData
					}));
				}
				else
					platform.handleException(error);
			});
		});
	}
	else if (isArray(data)) {
		async.each(data, function (datum) {
			processData(datum, (error, processedData) => {
				insertData(processedData, (error) => {
					if (!error) {
						platform.log(JSON.stringify({
							title: 'Record Successfully inserted to Cassandra Database.',
							data: processedData
						}));
					}
					else
						platform.handleException(error);
				});
			});
		});
	}
	else
		platform.handleException(new Error(`Invalid data received. Data must be a valid Array/JSON Object or a collection of objects. Data: ${data}`));
});

/*
 * Event to listen to in order to gracefully release all resources bound to this service.
 */
platform.on('close', function () {
	var d = require('domain').create();

	d.on('error', function (error) {
		platform.handleException(error);
		platform.notifyClose();
	});

	d.run(function () {
		/*cassandra.close(function () {
		 platform.notifyClose();
		 });*/
	});
});

/*
 * Listen for the ready event.
 */
platform.once('ready', function (options) {
	async.waterfall([
		async.constant(options.schema || '{}'),
		async.asyncify(JSON.parse)
	], (parseError, schemaObj) => {
		if (parseError || isEmpty(schemaObj)) {
			platform.handleException(new Error(`Invalid schema provided. Schema must be a valid JSON String that adheres to the Express Cassandra Schema (http://express-cassandra.readthedocs.io/en/latest/schema/).`));

			return setTimeout(() => {
				process.exit(1);
			}, 5000);
		}

		schema = schemaObj;

		let host = `${options.host}`.replace(/\s/g, '').split(',');

		let clientOptions = {
			contactPoints: host,
			keyspace: options.keyspace,
			protocolOptions: {
				port: options.port || 9042
			}
		};

		let ormOptions = {
			defaultReplicationStrategy: {
				class: 'SimpleStrategy',
				replication_factor: 1
			},
			migration: 'safe',
			createKeyspace: true
		};

		if (!isEmpty(options.replication_config)) {
			try {
				ormOptions.defaultReplicationStrategy = Object.assign({
					class: 'NetworkTopologyStrategy'
				}, JSON.parse(options.replication_config));
			}
			catch (ex) {
				platform.handleException(new Error(`Invalid replication config provided. Replication config must be a valid JSON String.`));

				return setTimeout(() => {
					process.exit(1);
				}, 5000);
			}
		}
		else if (!isNil(options.replication_factor))
			ormOptions.defaultReplicationStrategy.replication_factor = parseInt(options.replication_factor);

		if (options.port)
			clientOptions.protocolOptions = {port: options.port};

		var Cassandra = require('express-cassandra');

		cassandra = Cassandra.createClient({
			clientOptions: Object.assign(clientOptions, {
				authProvider: (!isEmpty(options.user) || !isEmpty(options.password)) ? new Cassandra.driver.auth.PlainTextAuthProvider(options.user, options.password) : undefined,
				queryOptions: {
					consistency: Cassandra.consistencies.one
				}
			}),
			ormOptions: ormOptions
		});

		cassandra.connect(function (connectionError) {
			if (connectionError) {
				platform.handleException(connectionError);

				return setTimeout(() => {
					process.exit(1);
				}, 5000);
			}

			Data = cassandra.loadSchema('Data', schemaObj, function (modelError) {
				if (modelError) {
					platform.handleException(modelError);

					setTimeout(() => {
						process.exit(1);
					}, 5000);
				} else {
					platform.log('Cassandra Storage plugin ready.');
					platform.notifyReady();
				}
			});
		});
	});
});