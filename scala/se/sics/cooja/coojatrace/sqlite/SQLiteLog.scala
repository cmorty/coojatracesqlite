package se.sics.cooja.coojatrace.rules.logrules


import se.sics.cooja._
import coojatrace._
import rules._
import logrules._

import java.util.{Observer, Observable}

import com.almworks.sqlite4java._



/**
 * Package for logging to a sqlite database.
 */
package sqlitelog {

/**
 * Wrapper class for a SQLite database.
 *
 * @param file SQLite database filename
 * @param sim current simulation
 */
case class SQLiteDB(file: String)(implicit sim: Simulation) {
  /**
   * DB connection. Lazily initialized to ensure this is called from the simulation thread
   * as the sqlite wrapper is not thread safe and refuses to work if initialized from another thread.
   */
  lazy val connection: SQLiteConnection = {
    // close db on plugin deactivation
    CoojaTracePlugin.forSim(sim).onCleanUp {
      connection.dispose()
    }
    
    // opens or create db
    new SQLiteConnection(new java.io.File(file)).open(true)
  }
}

/**
 * A [[LogDestination]] which writes into a sqlite table.
 *
 * @param db [[SQLiteQueue]] object for the database in which table is found
 * @param table database table to write to. Will be created or cleared if needed
 * @param columns list of (name -> type) tuples for table column definition. Type names as in
 *   sqlite SQL syntax
 * @param timeColumn (optional) column name for simulation time. When set to `null`, time column
 *   will not be logged, default: "Time"
 * @param sim the current [[Simulation]]
 */
case class LogTable(db: SQLiteDB, table: String, columns: List[(String, String)], timeColumn: String = "Time")(implicit sim: Simulation) extends LogDestination {
  // active as long as queue is running (i.e. db connection is open)
  def active = db.connection.isOpen
  
  /**
   * Logger.
   */
  val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  /**
   * Complete columns list (time added if not disabled).
   */
  val allColumns = if(timeColumn != null) ((timeColumn -> "long") :: columns) else columns

  /**
   * Column name list. Spaces replaced by underscores. Other characters not handled!
   */
  val colNames = allColumns.map(_._1.replace(" ", "_"))
  
  /**
   * Column type list.
   */
  val colTypes = allColumns.map(_._2)

  /** 
   * Column definition SQL.
   */
  val coldef = (colNames zip colTypes).map(c => c._1 + " " + c._2).mkString("(", ", ", ")")

  // recreate table, start transaction and save prepared INSERT statement
  lazy val insertStatement = {
    db.connection.exec("DROP TABLE IF EXISTS " + table)
    db.connection.exec("CREATE TABLE " + table + coldef)
    logger.info("Created table " + table)
    db.connection.exec("BEGIN")
    db.connection.prepare("INSERT INTO " + table + colNames.mkString("(", ", ", ")") +
                          " VALUES " + colNames.map(c => "?").mkString("(", ", ", ")"), true)
  }

  // add observer to Simulation which commits transaction when sim is stopped
  sim.addObserver(new Observer() {
    def update(obs: Observable, obj: Object) {
      if(!sim.isRunning) db.connection.exec("COMMIT")
    }
  })

  def log(values: List[_]) {
    // check for right number of columns
    require(values.size == columns.size, "incorrect column count")

    try {
      // bind value (and time if not disabled) to insert statement
      val start = if(timeColumn != null) {
        insertStatement.bind(1, sim.getSimulationTime)
        2
      } else {
        1
      }
      for((v, i) <- values.zipWithIndex) insertStatement.bind(i+start, v.toString)
      
      // execute statement
      insertStatement.step()
    } catch {
      case e: Exception => logger.error("DB exception: " + e)
    } finally {
      // reset bindings for next call
      insertStatement.reset()
    }
  }
}

} // package sqlitelog
