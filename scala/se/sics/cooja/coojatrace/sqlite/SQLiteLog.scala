package se.sics.cooja.coojatrace.rules.logrules


import se.sics.cooja._
import coojatrace._
import rules._
import logrules._

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
case class SQLiteDB(file: String)(implicit sim: Simulation) 
  extends SQLiteQueue(new java.io.File(file)) {
  // start job queue (opens or creates db)
  start()

  // stop queue / close db on plugin deactivation
  CoojaTracePlugin.forSim(sim).onCleanUp {
    this.stop(true)
  }

  /**
   * Convenience function for executing DB jobs.
   *
   * @param fun job function, called with SQLLiteConnection object
   * @return [[SQLiteJob]] object for obtaining results
   * @tparam result type of job
   */
  def exec[T](fun: SQLiteConnection => T): SQLiteJob[T] = {
    execute[T, SQLiteJob[T]](new SQLiteJob[T]() {
      protected def job(conn: SQLiteConnection): T = {
        fun(conn)
      }
    })
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
  def active = !db.isStopped
  
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

  // recreate table and save prepared INSERT statement
  val insertStatement = db.exec { conn =>
    conn.exec("DROP TABLE IF EXISTS " + table)
    conn.exec("CREATE TABLE " + table + coldef)
    conn.prepare("INSERT INTO " + table + colNames.mkString("(", ", ", ")") +
                 " VALUES " + colNames.map(c => "?").mkString("(", ", ", ")"), true)
  }.complete()
  logger.info("Created table " + table)

  def log(values: List[_]) {
    // check for right number of columns
    require(values.size == columns.size, "incorrect column count")

    db.exec { conn =>
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
}

} // package sqlitelog
