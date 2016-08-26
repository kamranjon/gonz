var Promise = require("bluebird"); 
var yaml = require('js-yaml');
var _ = require('lodash');
var fs = Promise.promisifyAll(require('fs'));
var pg = Promise.promisifyAll(require('pg'));


var config = {};
try {
  config = yaml.safeLoad(fs.readFileSync('config.yml', 'utf8'));

} catch (e) {
  console.log(e);
  process.exit();
}

var node_or_rel = new RegExp(`(^${config.db.node_prefix})|(^${config.db.rel_prefix})`);
var node_regex = new RegExp(`(^${config.db.node_prefix})`);
var rel_regex = new RegExp(`(^${config.db.rel_prefix})`);

var gcloud = require('google-cloud');
var bigquery = gcloud.bigquery({
  projectId: config.bigquery.ProjectId,
  keyFilename: config.bigquery.OAuthPvtKeyPath
});

var lines = [];
var page = config.db.page;
var current_timestamp = Date.now();

var pgJdbcFromHash = function(params = {}){
  var conn_params = ['user', 'password', 'ssl', 'sslfactory', 'sslfactoryarg', 'socketFactory', 'socketFactoryArg', 'compatible', 'sendBufferSize', 'recvBufferSize', 'protocolVersion', 'logLevel', 'charSet', 'allowEncodingChanges', 'logUnclosedConnections', 'binaryTransferEnable', 'binryTransferDisable', 'prepareThreshold', 'preparedStatementCacheQueries', 'preparedStatementCacheSizeMiB', 'defaultRowFetchSize', 'reWriteBatchedInserts', 'loginTimeout', 'connectTimeout', 'cancelSignalTimeout', 'socketTimeout', 'tcpKeepAlive', 'unknownLength', 'stringtype', 'kerberosServerName', 'jaasApplicationName', 'ApplicationName', 'gsslib', 'sspiServiceClass', 'useSpnego', 'sendBufferSize', 'receiveBufferSize', 'readOnly', 'disableColumnSanitiser', 'assumeMinServerVersion', 'currentSchema', 'targetServerType', 'hostRecheckSeconds', 'loadBalanceHosts']
  
  var provided_params = _.reduce(params, function(result, value, key){
    result = result || [];
    if(_.includes(conn_params, key)){
      result.push(`${key}=${value}`);
    }
    return result;
  }, []);
  var joined_conn_params = _.join(provided_params, '&');
  var jdbc_string = `jdbc:postgresql://${params.host}:${params.port}/${params.database}?${joined_conn_params}`;
  return jdbc_string;
}

var bqJdbcFromHash = function(params={}){
  var conn_params = ['AllowLargeResults', 'Catalog', 'LargeResultDataset', 'LargeResultTable', 'LogLevel', 'LogPath', 'MaxReqPerSec', 'MaxResults', 'OAuthClientId', 'OAuthClientSecret', 'OAuthPvtKeyPath', 'OAuthServiceAcctEmail', 'OAuthType', 'ProjectId', 'SQLDialect', 'Timeout']
  var provided_params = _.reduce(params, function(result, value, key){
    result = result || [];
    if(_.includes(conn_params, key)){
      result.push(`${key}=${value}`);
    }
    return result;
  }, []);
  var joined_conn_params = _.join(provided_params, ';');
  var jdbc_string = `jdbc:bigquery://${params.host}:${params.port};${joined_conn_params}`;
  return jdbc_string;
}

task('default', function(){
  console.log('thanks for checking out gonz, please provide an argument!');
})

var task_queries = new Map([
  ['index', `SELECT c.relname as name, json_agg(attr.attname) as attributes FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute attr ON attr.attrelid = ('${config.db.schema}.' || c.relname)::regclass AND    attr.attnum > 0 AND    NOT attr.attisdropped WHERE c.relkind IN ('m', 'v') AND n.nspname = '${config.db.schema}' and c.relname LIKE '${config.db.node_prefix}%' GROUP BY c.relname ORDER BY c.relname;`],
  ['nodes', `SELECT c.relname as name, json_agg(attr.attname) as attributes FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute attr ON attr.attrelid = ('${config.db.schema}.' || c.relname)::regclass AND    attr.attnum > 0 AND    NOT attr.attisdropped WHERE c.relkind IN ('m', 'v') AND n.nspname = '${config.db.schema}' and c.relname LIKE '${config.db.node_prefix}%' GROUP BY c.relname ORDER BY c.relname;`],
  ['rels', `SELECT c.relname as name, json_agg(attr.attname) as attributes FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute attr ON attr.attrelid = ('${config.db.schema}.' || c.relname)::regclass AND    attr.attnum > 0 AND    NOT attr.attisdropped WHERE c.relkind IN ('m', 'v') AND n.nspname = '${config.db.schema}' and c.relname LIKE '${config.db.rel_prefix}%' GROUP BY c.relname ORDER BY c.relname;`]
]);

task_queries.forEach(function(query, task_name){
  namespace('pg', function(){
    var file = `./graph_import_${current_timestamp}_pg_${task_name}.cql`;
    task(task_name, function(){
      let run = process.env.import;
      pg.connectAsync(config.postgres).then(function(client) {
        // grab all of the node and relationship views
        client.queryAsync(query).then(function(result){
          task_name === 'index' ? index_to_file(result, file, run) : append_results_to_file(result, file, run);
        })
        .finally(function(){
          client.release();
        });
      })
    });
  });
  namespace('bq', function(){
    var file = `./graph_import_${current_timestamp}_bq_${task_name}.cql`;
    task(task_name, function(){
      var dataset = Promise.promisifyAll(bigquery.dataset(config.db.schema));
      let run = process.env.import;
      switch(task_name){
        case 'all':
          dataset.getTablesAsync().then(function(tables, nextQuery, apiResponse) {
            append_results_to_file({rows:tables}, file, run);
          });
          break;
        case 'index': 
          dataset.getTablesAsync().then(function(tables, nextQuery, apiResponse) {
            tables = _.reduce(tables, function(sum, n){if(node_regex.test(n.metadata.tableReference.tableId)) sum.push(structureBgView(n)); return sum;}, [])
            index_to_file({rows:tables}, file, run);
          });
          break
        case 'nodes':
          dataset.getTablesAsync().then(function(tables, nextQuery, apiResponse) {
            tables = _.reduce(tables, function(sum, n){if(node_regex.test(n.metadata.tableReference.tableId)) sum.push(n); return sum;}, [])
            if(tables.length === 0) return;
            append_results_to_file({rows:tables}, file, run);
          });
          break;
        case 'rels':
          dataset.getTablesAsync().then(function(tables, nextQuery, apiResponse) {
            tables = _.reduce(tables, function(sum, n){if(rel_regex.test(n.metadata.tableReference.tableId)) sum.push(n); return sum;}, [])
            if(tables.length === 0) return;
            append_results_to_file({rows:tables}, file, run);
          });
          break;        
      }
    })
  })
});

namespace('bq', function(){
  task('all', function(){
    jake.Task['bq:index'].invoke();
    jake.Task['bq:nodes'].invoke();
    jake.Task['bq:rels'].invoke();
  });
  task('node', function(node_name){
    let run = process.env.import;
    node_file  = `./graph_import_${current_timestamp}_bq_node_${_.snakeCase(node_name)}.cql`;
    var dataset = Promise.promisifyAll(bigquery.dataset(config.db.schema));
    var node_table = dataset.table(`${config.db.node_prefix + _.snakeCase(node_name)}`);
    append_results_to_file({rows:[node_table]}, node_file, run);
  });

  task('rel', function(rel_name){
    let run = process.env.import;
    rel_file  = `./graph_import_${current_timestamp}_bq_rel_${_.snakeCase(rel_name)}.cql`;
    var dataset = Promise.promisifyAll(bigquery.dataset(config.db.schema));
    var rel_table = dataset.table(`${config.db.rel_prefix + _.snakeCase(rel_name)}`);
    append_results_to_file({rows:[rel_table]}, rel_file, run);
  });
});

namespace('pg', function(){
  task('all', function(){
    jake.Task['pg:index'].invoke();
    jake.Task['pg:nodes'].invoke();
    jake.Task['pg:rels'].invoke();
  });
  task('node', function(node_name){
    node_file  = `./graph_import_${current_timestamp}_pg_node_${_.snakeCase(node_name)}.cql`;
    let run = process.env.import;
    pg.connectAsync(config.postgres).then(function(client){
      // grab all of the node and relationship views
      let query = `SELECT c.relname as name, json_agg(attr.attname) as attributes FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute attr ON attr.attrelid = ('${config.db.schema}.' || c.relname)::regclass AND    attr.attnum > 0 AND    NOT attr.attisdropped WHERE c.relkind IN ('m', 'v') AND n.nspname = '${config.db.schema}' and c.relname LIKE '${config.db.node_prefix}%' AND c.relname = '${config.db.node_prefix + _.snakeCase(node_name)}' GROUP BY c.relname ORDER BY c.relname;`;
      client.queryAsync(query).then(function(result){
        append_results_to_file(result, node_file, run);
      }).finally(function(){
        client.release();
      });
    });
  });
   
  task('rel', function(rel_name){
    rel_file  = `./graph_import_${current_timestamp}_pg_rel_${_.snakeCase(rel_name)}.cql`;
    pg.connectAsync(config.postgres).then(function(client){
      // grab all of the node and relationship views
      let run = process.env.import;
      let update = process.env.update;
      let query = `SELECT c.relname as name, json_agg(attr.attname) as attributes FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute attr ON attr.attrelid = ('${config.db.schema}.' || c.relname)::regclass AND    attr.attnum > 0 AND    NOT attr.attisdropped WHERE c.relkind IN ('m', 'v') AND n.nspname = '${config.db.schema}' and c.relname LIKE '${config.db.rel_prefix}%' AND c.relname = '${config.db.rel_prefix + _.snakeCase(rel_name)}' GROUP BY c.relname ORDER BY c.relname;`;
      client.queryAsync(query).then(function(result){
        append_results_to_file(result, rel_file, run);
      }).finally(function(){
        client.release();
      });
    });
  });
})


namespace('neo', function(){
  task('import', function(){

  })
})

var upperCamelCase = function(string){
  return _.upperFirst(_.camelCase(string));
}

var structureBqView = function(view, metadata = []){
  bq_table_name = view.id;
  bq_attributes = _.map(metadata.schema.fields, function(n){return n.name});
  return {name:bq_table_name, attributes:bq_attributes, type:'bq', table:view};
}

var append_results_to_file = function(result, file, run = false){
  result.rows.forEach(function(view){
    lines.push(cypher_create_query(view));
  });

  Promise.all(lines).then(function (result) {
    Promise.map(result, function(statements){
      Promise.map(statements, function(statement){
        return fs.appendFileAsync(file, statement);
      });
    });
  }).finally(function(){
    if(run){
      jake.exec([`cat ${file} | ${config.neo4j.path}/bin/neo4j-shell`], {printStdout: true}, function(){
        console.log(`Finished Importing "${file}"`);
      });
    } else {
      console.log('finished!');
    }
  });
}

var cypher_create_query = function(view){
  // let's prepare our big query views
  if(view.id){
    view = Promise.promisifyAll(view);
    return view.getMetadataAsync().then(function(metadata, apiResponse){
      return delegate_partitions(structureBqView(view, metadata));
    });
  } else {
    return delegate_partitions(view);
  }
};

var delegate_partitions = function(view){
  if (view.name.match(`^${config.db.node_prefix}`)){
    return get_view_partitioned(view, node_query);
  } else if (view.name.match(`^${config.db.rel_prefix}`)){
    return get_view_partitioned(view, relationship_query);
  } else {
    console.log(`how did this table: ${view.name} get here?`)
  }
}

var get_view_partitioned = function(view, create_method) {
  return new Promise ( function (resolve, reject) {
    get_count(view, function(total_records){
      var promises = [];
      var total_fetches = Math.ceil((total_records / page) * 1) / 1;
      var offset = 0;
      for(i = 0;  i < total_fetches; i++){
        promises.push(create_method(view, page, offset));
        offset += page
      }
      return resolve(promises);
    });
  });
}

var get_count = function(view, callback){
  if(view.type === 'bq'){
    query = `SELECT count(1) as count FROM [${config.bigquery.ProjectId}.${config.db.schema}.${view.name}]`;
    return view.table.queryAsync(query).then( function(results){
      callback(results[0].count);
    });
  } else {
    return pg.connectAsync(config.postgres).then(function(client) {
      // grab the count from the table to partition imports
      client.queryAsync(`SELECT count(1) as total FROM ${config.db.schema}.${view.name}`).then(function(result) {
        callback(result.rows[0].total);
      }).finally(function(){
        client.release();
      });
    });
  }
}

var relationship_query = function(view, limit, offset, update = false){
  var name = _.toUpper(view.name.replace(/^relationship_/, ''));
  var from = upperCamelCase(view.attributes[0].replace(/_id.*/, ''));
  var to = upperCamelCase(view.attributes[1].replace(/_id.*/, ''));
  var operation = update ? 'MERGE' : 'CREATE';
  var jdbc_string = view.type === 'bq' ? bqJdbcFromHash(config.bigquery) : pgJdbcFromHash(config.postgres); 
  var qualified_table = view.type === 'bq' ? `\`${config.db.schema}.${view.name}\`` : `${config.db.schema}.${view.name}`;
  var cypher = `WITH "SELECT * FROM ${qualified_table} LIMIT ${limit} OFFSET ${offset} ORDER BY ${view.attributes[0]}" as sql, "${jdbc_string}" as url CALL apoc.periodic.iterate("CALL apoc.load.jdbc({url},{sql}) YIELD row RETURN row", "MATCH (n:${from} {id: {row}.${view.attributes[0]}}), (m:${to} {id: {row}.${view.attributes[1]}}) ${operation} (n)-[r:${name}]->(m) SET r += {row} return count(r)", {batchSize:${config.import.batch_size}, parallel:false, params: {sql:sql, url:url}}) yield batches return batches;\n`;
  return cypher;
};

var node_query = function(view, limit, offset, update = false){
  var name = upperCamelCase(view.name.replace(/^node_/, ''));
  var operation = update ? 'MERGE' : 'CREATE';
  var jdbc_string = view.type === 'bq' ? bqJdbcFromHash(config.bigquery) : pgJdbcFromHash(config.postgres); 
  var qualified_table = view.type === 'bq' ? `\`${config.db.schema}.${view.name}\`` : `${config.db.schema}.${view.name}`;
  var cypher = `WITH "SELECT * FROM ${qualified_table} LIMIT ${limit} OFFSET ${offset} ORDER BY id" as sql, "${jdbc_string}" as url call apoc.periodic.iterate("CALL apoc.load.jdbc({url},{sql}) YIELD row RETURN row", "${operation} (p:${name} {id: {row}.${view.attributes[0]}}) SET p += {row}", {batchSize:${config.import.batch_size}, parallel:${config.import.parallel}, params: {sql:sql, url:url}}) yield batches return batches;\n`
  return cypher;
};

var index_query = function(view){
  var name = upperCamelCase(view.name.replace(/^node_/, ''));
  var index_create = `CREATE INDEX ON :${name}(id);\n`;
  return index_create;
};

var index_to_file = function(result, file, run = false){
  indexes = [];
  result.rows.forEach(function(view){
    indexes.push(fs.appendFileAsync(file, index_query(view)));
  })
  Promise.all(indexes).then(function(result){
    if(run){
      jake.exec([`cat ${file} | ${config.neo4j.path}/bin/neo4j-shell`], {printStdout: true}, function(){
        console.log(`Finished Importing "${file}"`);
      });
    } else {
      console.log('finished!');
    }
  });
}


