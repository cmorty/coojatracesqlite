/*
 * Copyright (c) 2011, Florian Lukas
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer. 2. Redistributions in
 * binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package de.fau.cooja.plugins.coojatrace.rules.logrules



import se.sics.cooja._

import de.fau.cooja.plugins.coojatrace._
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
    new SQLiteConnection(new java.io.File(file)).open(true).exec("BEGIN")
  }
}

/**
 * A [[LogDestination]] which writes into a sqlite table.
 *
 * @param db [[SQLiteQueue]] object for the database in which table is found
 * @param table database table to write to. Will be created or cleared if needed
 * @param columns list of column names.
 * @param timeColumn (optional) column name for simulation time. When set to `null`, time column
 *   will not be logged, default: "Time"
 * @param sim the current [[Simulation]]
 */
case class LogTable(db: SQLiteDB, table: String, columns: List[String], timeColumn: String = "Time")(implicit sim: Simulation) extends LogDestination {
  // active as long as queue is running (i.e. db connection is open)
  def active = db.connection.isOpen
  
  /**
   * Logger.
   */
  val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  /**
   * Complete columns list (time added if not disabled).
   */
  val allColumns = if(timeColumn != null) (timeColumn :: columns) else columns

  /**
   * Column name list. Spaces replaced by underscores. Other characters not handled!
   */
  val colNames = allColumns.map(_.replace(" ", "_"))


  var uncommitedInserts = 0

  def commit() {
    db.connection.exec("COMMIT")
    db.connection.exec("BEGIN")
    uncommitedInserts = 0
  }

  // recreate table, start transaction and save prepared INSERT statement
  // this is lazy to create (lazy) db for reasons above
  lazy val insertStatement = {
    db.connection.exec("DROP TABLE IF EXISTS " + table)
    db.connection.exec("CREATE TABLE " + table + colNames.mkString("(", ", ", ")"))
    logger.info("Created table " + table)
    commit()
    db.connection.prepare("INSERT INTO " + table + colNames.mkString("(", ", ", ")") +
                          " VALUES " + colNames.map(c => "?").mkString("(", ", ", ")"), true)
  }

  // add observer to Simulation which commits transaction when sim is stopped
  sim.addObserver(new Observer() {
    def update(obs: Observable, obj: Object) {
      if(!sim.isRunning) commit()
    }
  })

  def log(values: List[_]) {
    // check for right number of columns
    require(values.size == columns.size, "incorrect column count")

    try {
      // bind value (and time if not disabled) to insert statement
      val start = if(timeColumn != null) {
        insertStatement.bind(1, sim.getSimulationTime)
        2 // bind values from seconds on
      } else {
        1 // bind values from first on
      }
      for((v, i) <- values.zipWithIndex) insertStatement.bind(i+start, v.toString)
      
      // execute statement
      insertStatement.step()

      // commit every 10000 inserts
      uncommitedInserts += 1
      if(uncommitedInserts >= 10000) commit()
    } catch {
      case e: Exception => logger.error("DB exception: " + e)
    } finally {
      // reset bindings for next call
      insertStatement.reset()
    }
  }
}

} // package sqlitelog
