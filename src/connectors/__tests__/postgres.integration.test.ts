import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { PostgresConnector } from '../postgres/index.js';
import { IntegrationTestBase, type TestContainer, type DatabaseTestConfig } from './shared/integration-test-base.js';
import type { Connector } from '../interface.js';

class PostgreSQLTestContainer implements TestContainer {
  constructor(private container: StartedPostgreSqlContainer) {}
  
  getConnectionUri(): string {
    return this.container.getConnectionUri();
  }
  
  async stop(): Promise<void> {
    await this.container.stop();
  }
}

class PostgreSQLIntegrationTest extends IntegrationTestBase<PostgreSQLTestContainer> {
  constructor() {
    const config: DatabaseTestConfig = {
      expectedSchemas: ['public', 'test_schema'],
      expectedTables: ['users', 'orders'],
      expectedTestSchemaTable: 'products',
      testSchema: 'test_schema',
      supportsStoredProcedures: true,
      expectedStoredProcedures: ['get_user_count', 'calculate_total_age']
    };
    super(config);
  }

  async createContainer(): Promise<PostgreSQLTestContainer> {
    const container = await new PostgreSqlContainer('postgres:15-alpine')
      .withDatabase('testdb')
      .withUsername('testuser')
      .withPassword('testpass')
      .start();
    
    return new PostgreSQLTestContainer(container);
  }

  createConnector(): Connector {
    return new PostgresConnector();
  }

  createSSLTests(): void {
    describe('SSL Connection Tests', () => {
      it('should handle SSL mode disable connection', async () => {
        const baseUri = this.connectionString;
        const sslDisabledUri = baseUri.includes('?') ? 
          `${baseUri}&sslmode=disable` : 
          `${baseUri}?sslmode=disable`;
        
        const sslDisabledConnector = new PostgresConnector();
        
        // Should connect successfully with sslmode=disable
        await expect(sslDisabledConnector.connect(sslDisabledUri)).resolves.not.toThrow();
        
        // Check SSL status - should be disabled (false)
        const result = await sslDisabledConnector.executeSQL('SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()');
        expect(result.rows).toHaveLength(1);
        expect(result.rows[0].ssl).toBe(false);
        
        await sslDisabledConnector.disconnect();
      });

      it('should handle SSL mode require connection', async () => {
        const baseUri = this.connectionString;
        const sslRequiredUri = baseUri.includes('?') ? 
          `${baseUri}&sslmode=require` : 
          `${baseUri}?sslmode=require`;
        
        const sslRequiredConnector = new PostgresConnector();
        
        // In test containers, SSL may not be supported, so we expect either success or SSL not supported error
        try {
          await sslRequiredConnector.connect(sslRequiredUri);
          
          // If connection succeeds, check SSL status - should be enabled (true)
          const result = await sslRequiredConnector.executeSQL('SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()');
          expect(result.rows).toHaveLength(1);
          expect(result.rows[0].ssl).toBe(true);
          
          await sslRequiredConnector.disconnect();
        } catch (error) {
          // If SSL is not supported by the test container, that's expected
          expect(error instanceof Error).toBe(true);
          expect((error as Error).message).toMatch(/SSL|does not support SSL/);
        }
      });
    });
  }

  async setupTestData(connector: Connector): Promise<void> {
    // Create test schema
    await connector.executeSQL('CREATE SCHEMA IF NOT EXISTS test_schema');
    
    // Create users table
    await connector.executeSQL(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INTEGER
      )
    `);

    // Create orders table
    await connector.executeSQL(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        total DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create products table in test_schema
    await connector.executeSQL(`
      CREATE TABLE IF NOT EXISTS test_schema.products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        price DECIMAL(10,2)
      )
    `);

    // Insert test data
    await connector.executeSQL(`
      INSERT INTO users (name, email, age) VALUES 
      ('John Doe', 'john@example.com', 30),
      ('Jane Smith', 'jane@example.com', 25),
      ('Bob Johnson', 'bob@example.com', 35)
      ON CONFLICT (email) DO NOTHING
    `);

    await connector.executeSQL(`
      INSERT INTO orders (user_id, total) VALUES 
      (1, 99.99),
      (1, 149.50),
      (2, 75.25)
      ON CONFLICT DO NOTHING
    `);

    await connector.executeSQL(`
      INSERT INTO test_schema.products (name, price) VALUES 
      ('Widget A', 19.99),
      ('Widget B', 29.99)
      ON CONFLICT DO NOTHING
    `);

    // Create test stored procedures using SQL language to avoid dollar quoting
    await connector.executeSQL(`
      CREATE OR REPLACE FUNCTION get_user_count()
      RETURNS INTEGER
      LANGUAGE SQL
      AS 'SELECT COUNT(*)::INTEGER FROM users'
    `);

    await connector.executeSQL(`
      CREATE OR REPLACE FUNCTION calculate_total_age()
      RETURNS INTEGER
      LANGUAGE SQL  
      AS 'SELECT COALESCE(SUM(age), 0)::INTEGER FROM users WHERE age IS NOT NULL'
    `);
  }
}

// Create the test suite
const postgresTest = new PostgreSQLIntegrationTest();

describe('PostgreSQL Connector Integration Tests', () => {
  beforeAll(async () => {
    await postgresTest.setup();
  }, 120000);

  afterAll(async () => {
    await postgresTest.cleanup();
  });

  // Include all common tests
  postgresTest.createConnectionTests();
  postgresTest.createSchemaTests();
  postgresTest.createTableTests();
  postgresTest.createSQLExecutionTests();
  if (postgresTest.config.supportsStoredProcedures) {
    postgresTest.createStoredProcedureTests();
  }
  postgresTest.createErrorHandlingTests();
  postgresTest.createSSLTests();
  describe('PostgreSQL-specific Features', () => {
    it('should execute multiple statements with transaction support', async () => {
      const result = await postgresTest.connector.executeSQL(`
        INSERT INTO users (name, email, age) VALUES ('Multi User 1', 'multi1@example.com', 30);
        INSERT INTO users (name, email, age) VALUES ('Multi User 2', 'multi2@example.com', 35);
        SELECT COUNT(*) as total FROM users WHERE email LIKE 'multi%';
      `);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].total).toBe('2');
    });

    it('should handle PostgreSQL-specific data types', async () => {
      await postgresTest.connector.executeSQL(`
        CREATE TABLE IF NOT EXISTS postgres_types_test (
          id SERIAL PRIMARY KEY,
          json_data JSONB,
          uuid_val UUID DEFAULT gen_random_uuid(),
          array_val INTEGER[],
          timestamp_val TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
      `);

      await postgresTest.connector.executeSQL(`
        INSERT INTO postgres_types_test (json_data, array_val) 
        VALUES ('{"key": "value"}', ARRAY[1,2,3,4,5])
      `);

      const result = await postgresTest.connector.executeSQL(
        'SELECT * FROM postgres_types_test ORDER BY id DESC LIMIT 1'
      );
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].json_data).toBeDefined();
      expect(result.rows[0].uuid_val).toBeDefined();
      expect(result.rows[0].array_val).toBeDefined();
    });

    it('should handle PostgreSQL returning clause', async () => {
      const result = await postgresTest.connector.executeSQL(
        "INSERT INTO users (name, email, age) VALUES ('Returning Test', 'returning@example.com', 40) RETURNING id, name"
      );
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].id).toBeDefined();
      expect(result.rows[0].name).toBe('Returning Test');
    });

    it('should work with PostgreSQL-specific functions', async () => {
      const result = await postgresTest.connector.executeSQL(`
        SELECT 
          version() as postgres_version,
          current_database() as current_db,
          current_user as current_user,
          now() as current_time,
          gen_random_uuid() as random_uuid
      `);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].postgres_version).toContain('PostgreSQL');
      expect(result.rows[0].current_db).toBe('testdb');
      expect(result.rows[0].current_user).toBeDefined();
      expect(result.rows[0].current_time).toBeDefined();
      expect(result.rows[0].random_uuid).toBeDefined();
    });

    it('should handle PostgreSQL transactions correctly', async () => {
      // Test rollback on error
      await expect(
        postgresTest.connector.executeSQL(`
          BEGIN;
          INSERT INTO users (name, email, age) VALUES ('Transaction Test', 'trans@example.com', 40);
          INSERT INTO users (name, email, age) VALUES ('Transaction Test', 'trans@example.com', 40); -- This should fail due to unique constraint
          COMMIT;
        `)
      ).rejects.toThrow();
      
      // Verify rollback worked
      const result = await postgresTest.connector.executeSQL(
        "SELECT COUNT(*) as count FROM users WHERE email = 'trans@example.com'"
      );
      expect(result.rows[0].count).toBe('0');
    });

    it('should handle PostgreSQL window functions', async () => {
      const result = await postgresTest.connector.executeSQL(`
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

    it('should handle PostgreSQL arrays and JSON operations', async () => {
      await postgresTest.connector.executeSQL(`
        CREATE TABLE IF NOT EXISTS json_test (
          id SERIAL PRIMARY KEY,
          data JSONB
        )
      `);

      await postgresTest.connector.executeSQL(`
        INSERT INTO json_test (data) VALUES 
        ('{"name": "John", "tags": ["admin", "user"], "settings": {"theme": "dark"}}'),
        ('{"name": "Jane", "tags": ["user"], "settings": {"theme": "light"}}')
      `);

      const result = await postgresTest.connector.executeSQL(`
        SELECT 
          data->>'name' as name,
          data->'tags' as tags,
          data#>>'{settings,theme}' as theme
        FROM json_test
        WHERE data @> '{"tags": ["admin"]}'
      `);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('John');
      expect(result.rows[0].theme).toBe('dark');
    });

  });
});