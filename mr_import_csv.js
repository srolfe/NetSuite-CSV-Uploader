/**
 * CSV Import Tool
 * ----------------
 * Accepts a CSV in the following format
 * internal_id | record_type | any_body_fields | you_want_to_update  | sublist_name | line_id | any_sublist_fields | you_want_to_update
 *
 * You can update multiple record types in a CSV file
 * You can update multiple body fields, sublists, lines and line fields in a single row - or in separate lines
 *
 * Add an integer script parameter, ID: custscript_import_csv_file
 * Set this to the CSV file's file cabinet ID to process
 * Set OUTPUT_FOLDER to a predefined folder for CSV job outputs
 *
 * @NScriptType MapReduceScript
 * @NApiVersion 2.1
 * @author Steve Rolfechild
 */
 define(['N/record', 'N/runtime', 'N/file', 'N/cache'], function(record, runtime, file, cache) {
	 
	 /** !--IMPORTANT--! Set output folder ID for response CSVs !--IMPORTANT--! */
	 const OUTPUT_FOLDER = 22039458;
	 
	 /** Determines if a value should be considered "empty" or containing no value */
	 function isEmpty(value) { return value == undefined || value == null || (typeof value == 'string' && (value.length == 0 || value == "")); }
	 
	 /**
	  * Loads CSV file headers and stores to cache
	  *
	  * @param f The file's file cabinet ID
	  */
	 function storeHeaders(f) {
		 let csvFile = file.load({ id: f });
		 let reader = csvFile.getReader();
		 let header = reader.readUntil('\n').trim();
		 if (header.indexOf('internal_id') < 0 || header.indexOf('record_type') < 0) throw 'Invalid import - missing required headers [ internal_id, record_type ]';
		 
		 let templateCache = cache.getCache({
			 name: 'csv_header_cache',
			 scope: cache.Scope.PRIVATE
		 });
		 templateCache.put({
			 key: 'headers',
			 value: header
		 });
		 templateCache.put({
			 key: 'filename',
			 value: csvFile.name
		 });
	 }
	 
	 /**
	  * Pulls headers from cache
	  *
	  * @return string The header line from the CSV file
	  */
	 function getHeaders() {
		 let templateCache = cache.getCache({
			  name: 'csv_header_cache',
			  scope: cache.Scope.PRIVATE
		  });
		  let headers = templateCache.get({
			  key: 'headers'
		  });
		  if (isEmpty(headers)) throw 'Unable to load headers from cache';
		  return headers;
	 }
	 
	 /**
	  * Pulls headers from cache
	  *
	  * @return string The header line from the CSV file
	  */
	 function outputFilename() {
		 let templateCache = cache.getCache({
			 name: 'csv_header_cache',
			 scope: cache.Scope.PRIVATE
		 });
		 let fname = templateCache.get({
			 key: 'filename'
		 });
		 if (isEmpty(fname)) throw 'Unable to load input filename from cache';
		 fname = fname.replace(/\.csv$/i, '');
		 fname += '_' + Math.floor(new Date().getTime() / 1000);
		 fname += '.csv';
		 return fname;
	 }
	 
	 /**
	  * Correct field value datatypes
	  *
	  * @param string value The value to parse
	  * @return (null | bool | integer | string) The value with the correct datatype
	  */
	 function parseValue(value) {
		 if (isEmpty(value) || (typeof value == 'string' && value == 'null')) {
			 value = null;
		 } else if (typeof value == 'string' && ['true', 'false'].indexOf(value.toLowerCase()) > -1) {
			 value = value.toLowerCase() == 'true' ? true : false;
		 } else if (parseInt(value) == value) {
			 value = parseInt(value);
		 }
		 return value;
	 }
	 
	 return {
		 
		 /**
		  * Store headers in cache, send CSV file into M/R process
		  */
		 getInputData: function() {
			 let csvFile = runtime.getCurrentScript().getParameter({ name: 'custscript_import_csv_file' });
			 if (isEmpty(csvFile)) throw 'Missing CSV file';
			 log.debug('Begin CSV import', 'CSV file: ' + csvFile);
			 
			 // Load CSV headers and cache them for processing the lines
			 storeHeaders(csvFile);
			 
			 return {
				 type: 'file',
				 id: csvFile
			 }
		 },
		 
		 /**
		  * Parse CSV line, process record update
		  */
		 map: function(context) {
			 let line = [];
			 let error_message = '';
			 try {
				 if (isEmpty(context.value)) throw 'Missing context';
				 
				 // Compare line to header raw value - skip if it's the same
				 line = context.value.trim();
				 let headers = getHeaders();
				 if (line == headers) {
					 log.debug('Header', line);
					 return;
				 }
				 
				 // Does line have minimum required columns? (internal ID & record type)
				 line = line.split(/\s*\,\s*/);
				 if (line.length < 2) throw 'Invalid format - less than 2 columns';
				 
				 // Convert to object
				 headers = headers.split(/\s*\,\s*/);
				 let lineO = line.reduce(function(a, e, k) { a[headers[k]] = parseValue(e); return a; }, {});
				 log.debug('Row', lineO);
				 
				 // Load record, make changes
				 let rec = record.load({
					 type: lineO.record_type,
					 id: lineO.internal_id,
					 isDynamic: false
				 });
				 
				 let sublist = null; let lineId = null;
				 for (let i = 0; i < headers.length; i++) {
					 let header = headers[i];
					 let value = lineO[header];
					 
					 // Process sublist variables
					 switch (header) {
						 // Skip base record info
						 case "internal_id": continue; break;
						 case "record_type": continue; break;
						 
						 // Store sublist name
						 case "sublist_name": {
							 sublist = value;
							 //lineId = null;
							 continue;
						 } break;
						 
						 // Store line ID
						 case "line_id": {
							 //if (sublist == null) throw 'Invalid format - setting line ID before sublist';
							  lineId = value;
							  continue;
						  } break;
					 }
					 
					 // Make changes to record
					 if (!isEmpty(value)) {
						  if (sublist == null) {
							  rec.setValue({
								  fieldId: header,
								  value: value
							  });
						  } else {
							  if (lineId == null) throw 'Missing line ID for sublist - but inside sublist ' + sublist;
							  let ssLineId = rec.findSublistLineWithValue({
								  sublistId: sublist,
								  fieldId: 'line',
								  value: parseValue(lineId)
							  });
							  if (isEmpty(ssLineId)) {
								  log.error('Unable to locate line ' + ssLineId, context.value);
								  continue;
							  }
							  rec.setSublistValue({
								  sublistId: sublist,
								  fieldId: header,
								  line: ssLineId,
								  value: value
							  });
						  }
					 }
				 }
				 
				 // Save
				 rec.save();
			 } catch (e) {
				 log.error('Unable to import row', e);
				 error_message = e.message;
			 }
			 
			 line.push(error_message);
			 
			 context.write({
				 //key: lineO.record_type + '|' + lineO.internal_id,
				 key: context.key,
				 value: line.join(', ')
			 });
		 },
		 
		 /**
		  * Output CSV result
		  */
		 summarize: function(context) {
			 // Output errors encountered
			 context.mapSummary.errors.iterator().each(function(key, error) {
				 log.error(key, error);
				 return true;
			 });
			 
			 let fname = outputFilename();
			 let csvFile = file.create({
				 name: fname,
				 fileType: file.Type.CSV,
				 folder: OUTPUT_FOLDER
			 });
			 let headers = getHeaders().split(/\s*\,\s*/);
			 headers.push('error_message');
			 csvFile.appendLine({ value: headers.join(', ') });
			 context.output.iterator().each(function(key, value) {
				 csvFile.appendLine({ value: value });
				 return true;
			 });
			 
			 csvFile.save();
			 log.debug('Output File', fname);
		 }
	 }
	 
 });
