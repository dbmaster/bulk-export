import com.branegy.dbmaster.model.*
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JDBCDialect
import org.apache.commons.io.IOUtils

connectionSrv = dbm.getService(ConnectionService.class)

def server_name    = p_database.split("\\.")[0]
def database_name  = p_database.split("\\.")[1]

RevEngineeringOptions options = new RevEngineeringOptions();
options.database = database_name
options.importIndexes = false
options.importViews = false
options.importProcedures = false

connectionInfo = connectionSrv.findByName(server_name)
connector = ConnectionProvider.getConnector(connectionInfo)

logger.info("Connecting to server ${server_name}")
dialect = connector.connect()
logger.info("Retrieving list of tables from database ${database_name}")
model = dialect.getModel(server_name, options)

connection = connector.getJdbcConnection(database_name)
dbm.closeResourceOnExit(connection)

println "<pre>"


if (p_action.equals("Export")) {
	println "-- SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"		
} else if (p_action.equals("Import to MySQL")) {
	println """SET FOREIGN_KEY_CHECKS = 0;
			   SET UNIQUE_CHECKS = 0;
               SET SESSION tx_isolation='READ-UNCOMMITTED';
               SET sql_log_bin = 0;\n\n"""
} else if (p_action.equals("Import to MSSQL")) {
	// NOPE for now
} else {
	throw new RuntimeException("Unexpected value for action ${p_action}")
}


 

model.tables.each { table  ->
    logger.info("Generate statement for table ${table.name}")
	
	if (p_action.equals("Export")) {
		println generateExport(database_name, table, dialect)
	} else if (p_action.equals("Import to MySQL")) {
		println "ALTER TABLE ${table.name} ENGINE=MyISAM;"
     	println "ALTER TABLE ${table.name} DISABLE KEYS;"

		println generateMySqlImport(database_name, table)
		
     	println "ALTER TABLE ${table.name} ENABLE KEYS;"
	} else if (p_action.equals("Import to MSSQL")) {
		println generateMsSqlImport(database_name, table)
	} else {
		throw new RuntimeException("Unexpected value for action ${p_action}")
	}
	
    println "\n\n\n"
}
println "</pre>"

connection.close()

def generateExport(String dbName, Table table, JDBCDialect dialect) {
    def tableName   =   table.name
    def columns     =   table.columns

    switch (dialect.getDialectName().toLowerCase()) {
        case "mysql":
            def allColumns = columns.collect { column -> column.isNullable() ? "    IFNULL(`${column.name}`,'')" : "    `${column.name}`" }.join(",\n")
            def query = "SELECT \n${allColumns}\n FROM `${dbName}`.`${tableName}`";
            if (p_max_rows!=null) {
                query += " LIMIT ${p_max_rows} \n"
            }
            query += "\nINTO OUTFILE '${p_output_folder}/${tableName}.dat'\n FIELDS TERMINATED BY 0x00 ESCAPED BY '\\\\' LINES TERMINATED BY '\\r\\n';"
            return query;
        default:
            throw new RuntimeException("Not implemented")
    }
}


def generateMySqlImport(String dbName, Table table) {
    def tableName   =   table.name
    def columns     =   table.columns
	
	// http://dev.mysql.com/doc/refman/5.6/en/load-data.html
	

	def query = """LOAD DATA INFILE '${p_import_folder}/${tableName}.dat'
                   INTO TABLE ${p_import_db}.${tableName}
                   FIELDS TERMINATED BY 0x00
	                      ESCAPED BY '\\\\'
                          LINES TERMINATED BY '\\r\\n'
                   ("""
    
	columns.eachWithIndex { column, idx -> 
		query = query + (idx>0 ? ',' : '') + (column.isNullable() ? "@imp_${idx}" : "`${column.name}`")
	}
	query+=") \n"
	if (query.contains("@imp")) {
		query = query + "\n SET ";
	}
	def firstColumn = true
	columns.eachWithIndex { column, idx -> 
		if (column.isNullable()) {
			if (firstColumn) {
				firstColumn=false
			} else {
				query = query + ","
			}
			query = query + "\n`${column.name}` = IF(@imp_${idx}='', NULL, @imp_${idx})"
		}
	}
	query+=";"
	return query;
}

def generateMsSqlImport(String dbName, Table table) {
    def tableName   =   table.name
    def columns     =   table.columns

    switch (dialect.getDialectName().toLowerCase()) {
        case "mysql":
            def allColumns = columns.collect { column -> column.isNullable() ? "    IFNULL(`${column.name}`,'')" : "    `${column.name}`" }.join(",\n")
            def query = "SELECT \n${allColumns}\n FROM `${dbName}`.`${tableName}`";
            if (p_max_rows!=null) {
                query += " LIMIT ${p_max_rows} \n"
            }
            query += "\nINTO OUTFILE '${p_output_folder}/${tableName}.dat'\n FIELDS TERMINATED BY 0x00 ESCAPED BY '\\\\' LINES TERMINATED BY '\\r\\n';"
            return query;
        default:
            throw new RuntimeException("Not implemented")
    }
}
/* 
BULK INSERT ${tableName}
FROM '${p_import_folder}/${tableName}.dat'
WITH
(
FIRSTROW = 1,
FIELDTERMINATOR = '0x00',
ROWTERMINATOR = '\n',
-- LASTROW=1000,
ERRORFILE = '${p_import_folder}/${tableName}_error.txt',
MAXERRORS=1000000000,
-- BATCHSIZE=10000
TABLOCK
)

*/