'use strict';

var platform      = require('./platform'),
	async         = require('async'),
	isNil         = require('lodash.isnil'),
	moment        = require('moment'),
	isEmpty       = require('lodash.isempty'),
	isArray       = require('lodash.isarray'),
	isNumber      = require('lodash.isnumber'),
	isString      = require('lodash.isstring'),
	isBoolean     = require('lodash.isboolean'),
	isPlainObject = require('lodash.isplainobject'),
	fieldMapping, client, tableName;

let insertData = function (data, callback) {
	let query = `insert into ${tableName} (${data.columns.join(', ')}) values (${data.values.join(', ')})`;

	client.execute(query, data.data, {prepare: true}, (insertError) => {
		callback(insertError);
	});
};

let processData = function (data, callback) {
	let keyCount      = 0,
		processedData = {
			columns: [],
			values: [],
			data: {}
		};

	async.forEachOf(fieldMapping, (field, key, done) => {
		keyCount++;

		processedData.columns.push(`"${key}"`);
		processedData.values.push(`:val${keyCount}`);

		let datum = data[field.source_field],
			processedDatum;

		if (!isNil(datum) && !isEmpty(field.data_type)) {
			try {
				if (field.data_type === 'String') {
					if (isPlainObject(datum))
						processedDatum = JSON.stringify(datum);
					else
						processedDatum = `${datum}`;
				}
				else if (field.data_type === 'Integer') {
					if (isNumber(datum))
						processedDatum = datum;
					else {
						let intData = parseInt(datum);

						if (isNaN(intData))
							processedDatum = datum; //store original value
						else
							processedDatum = intData;
					}
				}
				else if (field.data_type === 'Float') {
					if (isNumber(datum))
						processedDatum = datum;
					else {
						let floatData = parseFloat(datum);

						if (isNaN(floatData))
							processedDatum = datum; //store original value
						else
							processedDatum = floatData;
					}
				}
				else if (field.data_type === 'Boolean') {
					if (isBoolean(datum))
						processedDatum = datum;
					else {
						if ((isString(datum) && datum.toLowerCase() === 'true') || (isNumber(datum) && datum === 1))
							processedDatum = true;
						else if ((isString(datum) && datum.toLowerCase() === 'false') || (isNumber(datum) && datum === 0))
							processedDatum = false;
						else
							processedDatum = (datum) ? true : false;
					}
				}
				else if (field.data_type === 'Timestamp') {
					if (isEmpty(field.format) && moment(datum).isValid())
						processedDatum = moment(datum).toDate();
					else if (!isEmpty(field.format) && moment(datum, field.format).isValid())
						processedDatum = moment(datum, field.format).toDate();
					else if (!isEmpty(field.format) && moment(datum).isValid())
						processedDatum = moment(datum).toDate();
					else
						processedDatum = datum;
				} else if (field.data_type === 'Map') {
					try {
						if (isPlainObject(datum))
							processedDatum = datum;
						else
							processedDatum = JSON.parse(datum);
					}
					catch (e) {
						processedDatum = datum;
					}
				} else if (field.data_type === 'Set') {
					try {
						if (isArray(datum))
							processedDatum = datum;
						else
							processedDatum = JSON.parse(datum);
					}
					catch (e) {
						processedDatum = datum;
					}
				}
			}
			catch (e) {
				if (isPlainObject(datum))
					processedDatum = JSON.stringify(datum);
				else
					processedDatum = datum;
			}
		}
		else if (!isNil(datum) && isEmpty(field.data_type)) {
			if (isPlainObject(datum))
				processedDatum = JSON.stringify(datum);
			else
				processedDatum = `${datum}`;
		}
		else
			processedDatum = null;

		processedData.data[`val${keyCount}`] = processedDatum;

		done();
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
						data: data
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
							data: datum
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
		console.error(error);
		platform.handleException(error);
		platform.notifyClose();
	});

	d.run(function () {
		client.shutdown(function () {
			platform.notifyClose();
		});
	});
});

/*
 * Listen for the ready event.
 */
platform.once('ready', function (options) {
	var isEmpty   = require('lodash.isempty'),
		cassandra = require('cassandra-driver');

	tableName = options.table;

	async.waterfall([
		async.constant(options.field_mapping || '{}'),
		async.asyncify(JSON.parse),
		(obj, done) => {
			fieldMapping = obj;
			done();
		}
	], (parseError) => {
		if (parseError) {
			platform.handleException(new Error('Invalid field mapping. Must be a valid JSON String.'));

			return setTimeout(() => {
				process.exit(1);
			}, 5000);
		}

		async.forEachOf(fieldMapping, (field, key, done) => {
			if (isEmpty(field.source_field))
				done(new Error(`Source field is missing for ${key} in field mapping.`));
			else if (field.data_type && (field.data_type !== 'String' && field.data_type !== 'Integer' &&
				field.data_type !== 'Float' && field.data_type !== 'Boolean' &&
				field.data_type !== 'Timestamp' && field.data_type !== 'Map' && field.data_type !== 'Set')) {

				done(new Error(`Invalid Data Type for ${key}. Allowed data types are String, Integer, Float, Boolean, Timestamp, Map and Set.`));
			}
			else
				done();

		}, (fieldMapError) => {
			if (fieldMapError) {
				console.error('Error parsing JSON field mapping.', fieldMapError);
				platform.handleException(fieldMapError);

				return setTimeout(() => {
					process.exit(1);
				}, 5000);
			}

			var host = `${options.host}`.replace(/\s/g, '').split(',');
			var authProvider = new cassandra.auth.PlainTextAuthProvider(options.user, options.password);

			client = new cassandra.Client({
				contactPoints: host,
				authProvider: authProvider,
				keyspace: options.keyspace
			});

			if (options.port) client.protocolOptions = {port: options.port};

			client.connect((connectionError) => {
				if (connectionError) {
					console.error('Error connecting in Cassandra.', connectionError);
					platform.handleException(connectionError);

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