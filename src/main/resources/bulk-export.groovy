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
println "-- SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"

model.tables.each { table  ->
    logger.info("Generate statement for table ${table.name}")

    println generateSql(table, dialect)
	
	println "\n\n\n------------------\n\n\n"
}
println "</pre>"

connection.close()

def generateSql(Table table, JDBCDialect dialect) {
	def tableName   =   table.name
	def columns		=   table.columns

	switch (dialect.getDialectName().toLowerCase()) {
	case "mysql":
		def allColumns = 
			columns.collect { column ->
				column.isNullable() ? "    IFNULL(${column.name},'')" : "    ${column.name}"
			}.join(",\n")
        def query = """SELECT \n${allColumns}\n FROM ${database_name}.${tableName}";
		if (p_max_rows!=null) {
		     query += " LIMIT ${p_max_rows} \n"
		}
		query += "INTO OUTFILE '${p_output_folder}/{tableName}.dat'\n FIELDS TERMINATED BY 0x00 ESCAPED BY '\\' LINES TERMINATED BY '\r\n'"
		return query;
/*	   
   case "oracle":
       columnNames = columns.findAll { !it.type.matches("BFILE|LONG|BLOB|LONG BINARY|LONG RAW|XMLTYPE")  }.collect { "\"${it.name}\" like :search_terms" }
       return  "select * from \"${tableName}\" where (".toString() + columnNames.join (" or \n")+") and ROWNUM <= ${p_max_rows}"
   case "sqlserver":
       columnNames = columns.findAll { !it.type.matches("timestamp|image|xml|sql_variant")  }
                            .collect { "["+it.name+"] like :search_terms" }
       tableName = "["+table.getSchema()+"].["+ table.getSimpleName()+"]"
       return  "select top ${p_max_rows} * from ${tableName} with (NOLOCK) \nwhere ".toString() +
                columnNames.join (" or \n      ")
   case "nuodb":
       columnNames = columns.findAll { !it.type.matches("image|xml")  }.collect { it.name+ " like :search_terms" }
       return  "select * from ${tableName} \nwhere ".toString() + columnNames.join (" or \n      ") + " limit 0,${p_max_rows}"
   default:
       columnNames = columns.findAll { !it.type.matches("image|xml")  }.collect { it.name+" like :search_terms" }
       return  "select * from ${tableName} \nwhere ".toString() + columnNames.join (" or \n      ") + " limit 0,${p_max_rows}"
   }
   
   */ 
	default:
		throw new RuntimeException("Not implemented")
	}
}