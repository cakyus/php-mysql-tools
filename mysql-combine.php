<?php

/***
 * mysql-combine.php
 *    
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *    
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *    
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 *    
 **/

/**
 * Combine two database
 * Memasukkan data dari suatu database ke database lain.
 * - Yang dikombinasikan adalah table dengan nama yang sama pada kedua 
 *   database yang dibandingkan.
 * - Hanya field yang sama dalam kedua database yang diambil datanya.
 * - Column yang lebih panjang akan otomatis dipotong datanya.
 * - Primary atau Unique Key yang berbeda dapat mengakibatkan data tidak 
 *   diproses (di-insert dengan perintah INSERT IGNORE ..).
 * - Column tanpa default value akan otomatis diisi berdasarkan tipe Column.
 * 
 * @configurations see <CONFIGURATION> .. </CONFIGURATION>
 * @usage php mysql-combine.php
 * @depends PHP 5.3, MySQLi
 **/

class Connection {

  public $hostname;
	public $username;
	public $password;
	public $database;
	public $portNumber;
	
	private $handle;
	
	public function __construct() {
		// mysql default port number
		$this->portNumber = 3306;
	}
	
	public function open() {
		if (!$this->handle = mysqli_connect(
			  $this->hostname
			, $this->username
			, $this->password
			, $this->database
			, $this->portNumber
			)) {
			throw new Exception('Can not open database');
		}
	}
	
	public function exec($sql) {
	
		if (!$query = mysqli_query($this->handle, $sql)) {
			throw new Exception(mysqli_error($this->handle)."\r\n$sql");
		}
		
		return true;
	}
	
	public function query($sql) {
	
		if (!$query = mysqli_query($this->handle, $sql)) {
			throw new Exception(mysqli_error($this->handle)."\r\n$sql");
		}
		
		$rows = array();
		
		while ($row = mysqli_fetch_array ($query,  MYSQLI_ASSOC)) {
			$rows[] = $row;
		}
		
		return $rows;
	}
	
	public function escape($value) {
		return mysqli_real_escape_string($this->handle, $value);
	}
	
	public function getCatalog() {
		return new Catalog($this);
	}
}

class Catalog {
	
	private $connection;
	
	public function __construct($connection) {
		$this->connection = $connection;
	}
	
	/**
	 * Return array of object table
	 **/
	
	public function getTables() {
	
		$tables = array();
		
		$rows = $this->connection->query(
			'SHOW TABLES FROM '.$this->connection->database
			);
		
		foreach ($rows as $row) {
			$table = new Table($this->connection);
			$table->name = $row['Tables_in_'.$this->connection->database];
			$tables[$table->name] = $table;
		}
		
		return $tables;
	}
}

class Table {
	
	public $name;
	
	private $connection;
	
	public function __construct($connection) {
		$this->connection = $connection;
	}
	
	public function getColumns() {
		
		$columns = array();
		
		$rows = $this->connection->query(
			'SHOW COLUMNS FROM '.$this->name
			);
		
		foreach ($rows as $row) {
			$column = new Column($this->connection, $this);
			$column->load($row);
			$columns[$column->name] = $column;
		}
		
		return $columns;
	}
}

class Column {
	
	public $name;
	public $type;
	public $length;
	public $decimals;
	public $defaultValue;
	
	private $connection;
	private $table;
	
	public function __construct($connection, $table) {
		$this->connection = $connection;
		$this->table = $table;
	}
	
	public function load($column) {
	
		$this->name = $column['Field'];

		if (preg_match("/([^\(]+)(\(([0-9]+)(,([0-9]+))?\))?/", $column['Type'], $match)) {
			// 1,3,5
			if (isset($match[1])) {
				$this->type = $match[1];
			}
			if (isset($match[3])) {
				$this->length = $match[3];
			}
			if (isset($match[5])) {
				$this->decimals = $match[5];
			}
		}
		
		$this->defaultValue = $column['Default'];
	}
	
	public function escape($value) {
		
		if (in_array($this->type, array('decimal'))) {
			// number
			return $value;
		} elseif (in_array($this->type, array('varchar', 'char', 'date'))) {
			// string
			return '"'.$this->connection->escape($value).'"';
		} else {
			throw new Exception('Unhandled column type. "'.$this->type.'"');
		}
	}
}

class Test {
	
	public function start() {
		
		// <CONFIGURATION>
		// setup first connection
		$connection = new Connection;
		
		$connection->hostname = 'localhost';
		$connection->username = 'root';
		$connection->password = '';
		$connection->database = 'STRUCTURE';
		$connection->portNumber = 3306;
		
		$connection->open();
		$catalog = $connection->getCatalog();
		$tables = $catalog->getTables();
		
		// setup second connection
		$connection2 = new Connection;
		
		$connection2->hostname = 'localhost';
		$connection2->username = 'root';
		$connection2->password = '';
		$connection2->database = 'DATA';
		$connection2->portNumber = 3306;
		
		// </CONFIGURATION>
		
		$connection2->open();
		$catalog2 = $connection2->getCatalog();
		$tables2 = $catalog2->getTables();
		
		
		// compare tables
		$commands = array();
		foreach ($tables as $tableName => $table) {
			
			if (isset($tables2[$tableName])) {
				// both table are exists in both databases
				$commands[] = 'TRUNCATE '.$connection->database.'.'.$tableName;
				$columns = $table->getColumns();
				$columns2 = $tables2[$tableName]->getColumns();
				
				$commandColumnNames = array();
				$commandColumnNames2 = array();
				foreach ($columns as $columnName => $column) {
					$commandColumnNames[$columnName] = $columnName;
					if (isset($columns2[$columnName])) {
						$column2 = $columns2[$columnName];
						// @todo check column length
						if ($column->length <> $column2->length) {
							$commandColumnNames2[$columnName] = 'LEFT('.$columnName.','.$column->length.')';
						} else {
							$commandColumnNames2[$columnName] = $columnName;
						}
					} else {
					
						$columnValue = NULL;
						
						if (in_array($column->type, array('decimal', 'int', 'mediumblob'))) {
							$columnValue = 0;
						} elseif (in_array($column->type, array('datetime'))) {
							$columnValue = '"0000-00-00 00:00:00"';
						} elseif (in_array($column->type, array('date'))) {
							$columnValue = '"0000-00-00"';
						} elseif (in_array($column->type, array('varchar', 'char'))) {
							$columnValue = '""';
						} else {
							throw new Exception('Unhandled column type. "'.$column->type.'"');
						}
						
						$commandColumnNames2[$columnName] = $columnValue;
					}
				}
				
				$sql = 'INSERT IGNORE INTO '.$connection->database.'.'.$tableName
					.' ('.implode(", ", $commandColumnNames).')'
					.' SELECT '.implode(", ", $commandColumnNames2)
					.' FROM '.$connection2->database.'.'.$tableName
					;
					
				$commands[] = $sql;
			}
		}
		
		echo "Executing commands ..\n";
		foreach ($commands as $sql) {
			try {
				$connection->exec($sql);
			} catch(Exception $e) {
				echo "Runtime Error: ".$e->getMessage()."\r\n"; die();
			}
		}
	}
}

$test = new Test;
$test->start();
