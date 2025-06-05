#!/usr/bin/env node

// A simple test script for the SQLite connector
// Run: node test-sqlite.js

import sqlite3 from 'sqlite3';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Create a test SQLite database
const db = new sqlite3.Database(':memory:');

// Create a test table
db.serialize(() => {
  console.log('Creating test table...');
  db.run(`CREATE TABLE test_table (
    id INTEGER PRIMARY KEY, 
    name TEXT NOT NULL,
    age INTEGER,
    email TEXT
  )`);

  // Insert some test data
  const stmt = db.prepare('INSERT INTO test_table (name, age, email) VALUES (?, ?, ?)');
  console.log('Inserting test data...');
  stmt.run('Alice', 28, 'alice@example.com');
  stmt.run('Bob', 35, 'bob@example.com');
  stmt.run('Charlie', 42, 'charlie@example.com');
  stmt.finalize();

  // Query the data
  console.log('\nTesting table schema:');
  db.all('PRAGMA table_info(test_table)', (err, rows) => {
    if (err) {
      console.error('Error getting table schema:', err);
      return;
    }
    console.log('Table schema:');
    rows.forEach((row) => {
      console.log(
        `  - ${row.name}: ${row.type} (nullable: ${row.notnull === 0 ? 'YES' : 'NO'}, default: ${row.dflt_value || 'NULL'})`
      );
    });

    console.log('\nTesting query execution:');
    db.all('SELECT * FROM test_table', (err, rows) => {
      if (err) {
        console.error('Error executing query:', err);
        return;
      }
      console.log('Query results:');
      rows.forEach((row) => {
        console.log(`  [${row.id}] ${row.name}, ${row.age}, ${row.email}`);
      });

      // Close the database
      db.close();

      console.log('\nTest completed successfully!');
      console.log('Now you can test DBHub with:');
      console.log('1. Create a .env file with: DSN=sqlite:///:memory:');
      console.log('2. Run: pnpm run dev');
    });
  });
});
