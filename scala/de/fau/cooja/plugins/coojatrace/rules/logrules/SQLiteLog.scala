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
import se.sics.cooja.Simulation._
import scala.actors._
import scala.actors.Actor._


import de.fau.cooja.plugins.coojatrace._
import rules._
import logrules._

import java.util.{Observer, Observable}
import java.util.concurrent.Semaphore

import com.almworks.sqlite4java._


// speed up compilation
class SQLiteLog



/**
 * Package for logging to a sqlite database.
 */
package sqlitelog {

  
  case class PrepareStmntQuery(query : String)
  case class PrepareStmnt(stmt : SQLiteStatement)
  case class PrepareData(stmt: SQLiteStatement, values: List[_])
  case class PrepareStmntDispose(stmt : SQLiteStatement)
  
  case object Close
  case object Commit
  
/**
 * Wrapper class for a SQLite database.
 *
 * @param file SQLite database filename
 * @param sim current simulation
 */
case class SQLiteDB(file: String)(implicit sim: Simulation) extends Actor {
   
     
    /**
   * Logger.
   */
  val log = org.apache.log4j.Logger.getLogger(this.getClass)

  // 30 should be enough to ensure efficiant work
  val sem = new Semaphore(30) 
  
  log.info("Created DB-Object: " + file)
  
  start()
    
  /**
   * DB connection. Lazily initialized to ensure this is called from the simulation thread
   * as the sqlite wrapper is not thread safe and refuses to work if initialized from another thread.
   */
  var active = false
  var forceflush = false
  
  val flush = 1000
  
  

  
  
  def act() {
    sim.addObserver(new Observer() {
		def update(obs: Observable, obj: Object) {
			if(!sim.isRunning()){
			   send(Commit, Actor.self) 
			} 
		}
	})
	
	log.info("Started DB-Actor")
    var uncommit = 0
    
    connection
	while(active){
		if( uncommit >= flush || forceflush){		 
		    log.info("Flushing " + uncommit + " entries")
		    uncommit = 0;
		    forceflush = false
		    connection.exec("COMMIT")
		    connection.exec("BEGIN")
		}
	  
		receive {
          case str:String => connection.exec(str)          
          case Close => active = false 
          case Commit => forceflush = true
          case prep:PrepareStmntQuery => 
          	sender ! PrepareStmnt(connection.prepare(prep.query, true))
          case pdata:PrepareData =>
            for((v, i) <- pdata.values.zipWithIndex){
              pdata.stmt.bind(1 + i, v.toString)
            } 
            pdata.stmt.step
            pdata.stmt.reset
            uncommit += 1
            sem.release()
          case disp:PrepareStmntDispose =>
            disp.stmt.dispose()
          case _ => {log.error("Got unexpected class!")}
		}
    }
    active = false
    connection.exec("COMMIT")
    connection.dispose
    log.info("Stopped DB-Actor")
    
  }
    
   
  
  private lazy val connection: SQLiteConnection = {
    log.info("Init CON:" + file.toString() + "  Obj:" + this.toString + " Hash:" + this.hashCode)
    val mthis = this

    // close db on plugin deactivation
    CoojaTracePlugin.forSim(sim).onCleanUp {
	    this ! Close
    }
    
    sim.addObserver(new Observer() {
		def update(obs: Observable, obj: Object) {
			if(!sim.isRunning){
			   mthis ! Commit 
			} 
		}
	})
    
    // opens or create db
    val conn = new SQLiteConnection(new java.io.File(file)).open(true)
    //If something crashes the data is worthless. This improves the performance significantly. 
    conn.exec("PRAGMA synchronous = OFF")
    conn.exec("BEGIN")
    active = true
    conn
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
  
  def active = db.active

  /**
   * Logger.
   */
  val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  logger.info("Created Table-Object: " + table)
  
  /**
   * Complete columns list (time added if not disabled).
   */
  val allColumns = if(timeColumn != null) (timeColumn :: columns) else columns

  /**
   * Column name list. Spaces replaced by underscores. Other characters not handled!
   */
  val colNames = allColumns.map(_.replace(" ", "_"))

  

  // recreate table, start transaction and save prepared INSERT statement
  // this is lazy to avoid imideate deletion of the database
  private lazy val insertStatement:SQLiteStatement = {
    db ! "DROP TABLE IF EXISTS " + table
    db ! "CREATE TABLE " + table + colNames.mkString("(", ", ", ")")
    logger.info("Creating Table " + table)
    db ! Commit

    db ! PrepareStmntQuery("INSERT INTO " + table + colNames.mkString("(", ", ", ")") +
                          " VALUES " + colNames.map(c => "?").mkString("(", ", ", ")"))
    receiveWithin(10000){
      case rep:PrepareStmnt => 
      	CoojaTracePlugin.forSim(sim).onCleanUp {
      		db ! PrepareStmntDispose(rep.stmt)
      	}
        rep.stmt
      case TIMEOUT => throw new UnsupportedOperationException("Unable to get prepared statement!"); 
    }
  }



  def log(values: List[_]) {	  
      // check for right number of columns
	  require(values.size == columns.size, "incorrect column count")
    
	  if (!db.sem.tryAcquire()){
	    logger.warn("The Database is slowing us down")
	    db.sem.acquire() //Make sure we slow things down until we get a semaphore
	  }
	  	  
	  // bind value (and time if not disabled) to insert statement
	  val data = if(timeColumn != null) {
	    sim.getSimulationTime :: values
	  } else {
	    values
	  }
	  	  
	  // execute statement
	  db ! PrepareData(insertStatement, data)
    
  }
}

} // package sqlitelog
