'use strict';

var platform      = require('./platform'),
	cassandra 	  = require('cassandra-driver'),
	async 		  = require('async'),
	isPlainObject = require('lodash.isplainobject'),
	isEmpty 	  = require('lodash.isempty'),
	moment  	  = require('moment'),
	parseFields, client, tableName;

/*
 * Listen for the data event.
 */
platform.on('data', function (data) {

	var columnList,
		valueList,
		valueRef = {},
		first = true;

	async.forEachOf(parseFields, function(field, key, callback) {

		var datum = data[field.source_field],
			processedDatum;

		if (datum !== undefined && datum !== null) {
			if (field.data_type) {
				try {
					if (field.data_type === 'String') {
						if (isPlainObject(datum))
							processedDatum = JSON.stringify(datum);
						else
							processedDatum = String(datum);

					} else if (field.data_type === 'Integer')  {

						var intData = parseInt(datum);

						if (isNaN(intData))
							processedDatum = datum; //store original value
						else
							processedDatum = intData;

					} else if (field.data_type === 'Float')  {

						var floatData = parseFloat(datum);

						if (isNaN(floatData))
							processedDatum = datum; //store original value
						else
							processedDatum = floatData;

					} else if (field.data_type === 'Boolean') {

						var type = typeof datum;

						if ((type === 'string' && datum.toLocaleLowerCase() === 'true') ||
							(type === 'number' && datum === 1 )) {
							processedDatum = true;
						} else if ((type === 'string' && datum.toLocaleLowerCase() === 'false') ||
							(type === 'number' && datum === 0 )) {
							processedDatum = false;
						} else {
							processedDatum = datum;
						}
					} else if (field.data_type === 'DateTime') {

						var dtm = new Date(datum);
						if (!isNaN( dtm.getTime())) {

							if (field.format !== undefined)
								processedDatum = moment(dtm).format(field.format);
							else
								processedDatum = dtm;


						} else {
							processedDatum = datum;
						}
					} else if (field.data_type === 'JSON') {
						try {
							JSON.parse(datum);
							if (isPlainObject(datum))
								processedDatum = datum;
							else
								processedDatum = JSON.parse(datum);

							console.log('in try');
						}
						catch(e) {
							processedDatum = datum;
						}
					}
				} catch (e) {
					if (typeof datum === 'number')
						processedDatum = datum;
					else if (isPlainObject(datum))
						processedDatum = JSON.stringify(datum);
					else
						processedDatum = datum ;
				}

			} else {
				if (typeof datum === 'number')
					processedDatum = datum;
				else if (isPlainObject(datum))
					processedDatum = JSON.stringify(datum);
				else
					processedDatum = datum ;
			}

		} else {
			processedDatum = null;
		}

		valueRef[key] = processedDatum;

		if (!first) {
			valueList  = valueList  + ',:' + key;
			columnList = columnList  + ',' + key;
		} else {
			first      = false;
			valueList  = ':' + key;
			columnList = key;
		}

		callback();

	}, function() {
		client.execute( 'insert into ' + tableName + ' (' + columnList + ') values (' + valueList + ')', valueRef, {prepare: true}, function(reqErr, queryset) {
			if (reqErr) {
			    console.error('Error creating record on Cassandra', reqErr);
			    platform.handleException(reqErr);
			} else {
			    platform.log(JSON.stringify({
			        title: 'Record Successfully inserted to Cassandra.',
			        data: valueRef
			    }));
			}
		});
	});

});

/*
 * Event to listen to in order to gracefully release all resources bound to this service.
 */
platform.on('close', function () {
	var domain = require('domain');
	var d = domain.create();

	d.on('error', function(error) {
		console.error(error);
		platform.handleException(error);
		platform.notifyClose();
	});

	d.run(function() {
		// TODO: Release all resources and close connections etc.
		platform.notifyClose(); // Notify the platform that resources have been released.
	});
});

/*
 * Listen for the ready event.
 */
platform.once('ready', function (options) {

	var init = function(e){
		if (e) {
			console.error('Error parsing JSON field configuration for Cassandra Plugin.', e);
			return platform.handleException(e);
		}

		var hostList = options.host.split(',');

		for (var i = 0; i < hostList.length; i++) {
			hostList[i] = hostList[i].trim();
		}

		var authProvider = new cassandra.auth.PlainTextAuthProvider(options.user, options.password);

		client = new cassandra.Client({
			contactPoints: hostList,
			authProvider: authProvider,
			keyspace: options.keyspace
		});

		if (options.port) client.protocolOptions = {port: options.port};
		tableName = options.table;
		client.connect(function(err) {

			if (err) {
				console.error('Error connecting in Cassandra.', err);
				platform.handleException(err);

			} else {
				platform.log('Cassandra Storage plugin ready.');
				platform.notifyReady();
			}
		});
	};


	parseFields =  JSON.parse(options.fields);

	async.forEachOf(parseFields, function(field, key, callback) {
		if (isEmpty(field.source_field)){
			callback( new Error('Source field is missing for ' + key + ' in Cassandra Plugin'));
		} else if (field.data_type  && (field.data_type !== 'String' && field.data_type !== 'Integer' &&
			field.data_type !== 'Float'  && field.data_type !== 'Boolean' &&
			field.data_type !== 'DateTime' && field.data_type !== 'JSON')) {
			callback(new Error('Invalid Data Type for ' + key + ' allowed data types are (String, Integer, Float, Boolean, DateTime, JSON) in Cassandra Plugin'));
		} else
			callback();

	}, init);




});