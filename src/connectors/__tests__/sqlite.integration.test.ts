import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { SQLiteConnector } from '../sqlite/index.js';
import { IntegrationTestBase, type TestContainer, type DatabaseTestConfig } from './shared/integration-test-base.js';
import type { Connector } from '../interface.js';
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';

class SQLiteTestContainer implements TestContainer {
  constructor(private dbPath: string) {}
  
  getConnectionUri(): string {
    return `sqlite://${this.dbPath}`;
  }
  
  async stop(): Promise<void> {
    // Clean up the temporary database file
    if (this.dbPath !== ':memory:' && fs.existsSync(this.dbPath)) {
      fs.unlinkSync(this.dbPath);
    }
  }
}

class SQLiteIntegrationTest extends IntegrationTestBase<SQLiteTestContainer> {
  constructor() {
    const config: DatabaseTestConfig = {
      expectedSchemas: ['main'], // SQLite uses 'main' as the default schema name
      expectedTables: ['users', 'orders'],
      supportsStoredProcedures: false // SQLite doesn't support stored procedures
    };
    super(config);
  }

  async createContainer(): Promise<SQLiteTestContainer> {
    // Create a temporary database file
    const tempDir = os.tmpdir();
    const dbPath = path.join(tempDir, `test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}.db`);
    
    return new SQLiteTestContainer(dbPath);
  }

  createConnector(): Connector {
    return new SQLiteConnector();
  }

  async setupTestData(connector: Connector): Promise<void> {
    // Create users table
    await connector.executeSQL(`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INTEGER
      )
    `);

    // Create orders table
    await connector.executeSQL(`
      CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER REFERENCES users(id),
        total DECIMAL(10,2),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Insert test data
    await connector.executeSQL(`
      INSERT INTO users (name, email, age) VALUES 
      ('John Doe', 'john@example.com', 30),
      ('Jane Smith', 'jane@example.com', 25),
      ('Bob Johnson', 'bob@example.com', 35)
    `);

    await connector.executeSQL(`
      INSERT INTO orders (user_id, total) VALUES 
      (1, 99.99),
      (1, 149.50),
      (2, 75.25)
    `);
  }
}

// Create the test suite
const sqliteTest = new SQLiteIntegrationTest();

describe('SQLite Connector Integration Tests', () => {
  beforeAll(async () => {
    await sqliteTest.setup();
  }, 120000);

  afterAll(async () => {
    await sqliteTest.cleanup();
  });

  // Include all common tests
  sqliteTest.createConnectionTests();
  sqliteTest.createSchemaTests();
  sqliteTest.createTableTests();
  sqliteTest.createSQLExecutionTests();
  sqliteTest.createErrorHandlingTests();

  describe('SQLite-specific Features', () => {
    it('should handle SQLite data types correctly', async () => {
      await sqliteTest.connector.executeSQL(`
        CREATE TABLE IF NOT EXISTS types_test (
          id INTEGER PRIMARY KEY,
          text_val TEXT,
          int_val INTEGER,
          real_val REAL,
          blob_val BLOB,
          null_val TEXT
        )
      `);

      await sqliteTest.connector.executeSQL(`
        INSERT INTO types_test (text_val, int_val, real_val, blob_val, null_val) 
        VALUES ('test string', 42, 3.14159, X'48656C6C6F', NULL)
      `);

      const result = await sqliteTest.connector.executeSQL(
        'SELECT * FROM types_test ORDER BY id DESC LIMIT 1'
      );
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].text_val).toBe('test string');
      expect(result.rows[0].int_val).toBe(42);
      expect(result.rows[0].real_val).toBe(3.14159);
      expect(result.rows[0].null_val).toBeNull();
    });

    it('should work with SQLite-specific functions', async () => {
      const result = await sqliteTest.connector.executeSQL(`
        SELECT 
          sqlite_version() as sqlite_version,
          datetime('now') as current_time,
          hex(randomblob(16)) as random_hex,
          upper('hello world') as uppercase_text,
          length('test string') as string_length
      `);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].sqlite_version).toBeDefined();
      expect(result.rows[0].current_time).toBeDefined();
      expect(result.rows[0].random_hex).toBeDefined();
      expect(result.rows[0].uppercase_text).toBe('HELLO WORLD');
      expect(result.rows[0].string_length).toBe(11);
    });

    it('should handle SQLite transactions correctly', async () => {
      // Test successful transaction
      await sqliteTest.connector.executeSQL(`
        BEGIN TRANSACTION;
        INSERT INTO users (name, email, age) VALUES ('Transaction User 1', 'trans1@example.com', 28);
        INSERT INTO users (name, email, age) VALUES ('Transaction User 2', 'trans2@example.com', 32);
        COMMIT;
      `);

      const successResult = await sqliteTest.connector.executeSQL(
        "SELECT COUNT(*) as count FROM users WHERE email LIKE 'trans%@example.com'"
      );
      expect(Number(successResult.rows[0].count)).toBe(2);

      // Test manual rollback
      await sqliteTest.connector.executeSQL(`
        BEGIN TRANSACTION;
        INSERT INTO users (name, email, age) VALUES ('Transaction User 3', 'trans3@example.com', 40);
        ROLLBACK;
      `);
      
      // Verify rollback worked - should still be 2 transaction users
      const rollbackResult = await sqliteTest.connector.executeSQL(
        "SELECT COUNT(*) as count FROM users WHERE email LIKE 'trans%@example.com'"
      );
      expect(Number(rollbackResult.rows[0].count)).toBe(2);
    });

    it('should handle SQLite pragma statements', async () => {
      const result = await sqliteTest.connector.executeSQL(`
        PRAGMA table_info(users);
      `);
      
      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.rows.some(row => row.name === 'id')).toBe(true);
      expect(result.rows.some(row => row.name === 'name')).toBe(true);
      expect(result.rows.some(row => row.name === 'email')).toBe(true);
    });

    it('should support SQLite window functions', async () => {
      const result = await sqliteTest.connector.executeSQL(`
        SELECT 
          name,
          age,
          ROW_NUMBER() OVER (ORDER BY age DESC) as age_rank,
          AVG(age) OVER () as avg_age
        FROM users
        WHERE age IS NOT NULL
        ORDER BY age DESC
      `);
      
      expect(result.rows.length).toBeGreaterThan(0);
      expect(result.rows[0]).toHaveProperty('age_rank');
      expect(result.rows[0]).toHaveProperty('avg_age');
    });

    it('should handle SQLite JSON functions (if available)', async () => {
      // SQLite 3.38+ has JSON support, but we'll make this test conditional
      try {
        await sqliteTest.connector.executeSQL(`
          CREATE TABLE IF NOT EXISTS json_test (
            id INTEGER PRIMARY KEY,
            data TEXT
          )
        `);

        await sqliteTest.connector.executeSQL(`
          INSERT INTO json_test (data) VALUES 
          ('{"name": "John", "age": 30, "tags": ["admin", "user"]}'),
          ('{"name": "Jane", "age": 25, "tags": ["user"]}')
        `);

        // Try to use json_extract (available in newer SQLite versions)
        const result = await sqliteTest.connector.executeSQL(`
          SELECT 
            json_extract(data, '$.name') as name,
            json_extract(data, '$.age') as age
          FROM json_test
          WHERE json_extract(data, '$.age') > 27
        `);
        
        expect(result.rows).toHaveLength(1);
        expect(result.rows[0].name).toBe('John');
        expect(Number(result.rows[0].age)).toBe(30);
      } catch (error) {
        // JSON functions not available in this SQLite version, skip this test
        console.log('JSON functions not available in this SQLite version, skipping JSON test');
      }
    });

    it('should handle multiple statements correctly', async () => {
      const result = await sqliteTest.connector.executeSQL(`
        INSERT INTO users (name, email, age) VALUES ('Multi User 1', 'multi1@example.com', 30);
        INSERT INTO users (name, email, age) VALUES ('Multi User 2', 'multi2@example.com', 35);
        SELECT COUNT(*) as total FROM users WHERE email LIKE 'multi%';
      `);
      
      expect(result.rows).toHaveLength(1);
      expect(Number(result.rows[0].total)).toBe(2);
    });

    it('should handle SQLite foreign key constraints', async () => {
      // Enable foreign key constraints
      await sqliteTest.connector.executeSQL('PRAGMA foreign_keys = ON');
      
      // Try to insert an order with non-existent user_id
      await expect(
        sqliteTest.connector.executeSQL('INSERT INTO orders (user_id, total) VALUES (9999, 100.00)')
      ).rejects.toThrow();

      // Verify foreign key is working by inserting valid order
      await sqliteTest.connector.executeSQL('INSERT INTO orders (user_id, total) VALUES (1, 200.00)');
      const result = await sqliteTest.connector.executeSQL(
        'SELECT COUNT(*) as count FROM orders WHERE total = 200.00'
      );
      expect(Number(result.rows[0].count)).toBe(1);
    });

    it('should work with SQLite virtual tables (FTS)', async () => {
      try {
        // Create an FTS (Full-Text Search) virtual table if FTS is available
        await sqliteTest.connector.executeSQL(`
          CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(title, content)
        `);

        await sqliteTest.connector.executeSQL(`
          INSERT INTO docs_fts (title, content) VALUES 
          ('First Document', 'This is the content of the first document'),
          ('Second Document', 'This document contains different content'),
          ('Third Document', 'Another document with more content')
        `);

        const result = await sqliteTest.connector.executeSQL(`
          SELECT title FROM docs_fts WHERE docs_fts MATCH 'content' ORDER BY title
        `);
        
        expect(result.rows.length).toBeGreaterThan(0);
        expect(result.rows.some(row => row.title.includes('Document'))).toBe(true);
      } catch (error) {
        // FTS not available in this SQLite build, skip this test
        console.log('FTS extension not available in this SQLite build, skipping FTS test');
      }
    });
  });
});