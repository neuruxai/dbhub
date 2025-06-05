import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { executeSqlToolHandler } from '../execute-sql.js';
import { ConnectorManager } from '../../connectors/manager.js';
import { isReadOnlyMode } from '../../config/env.js';
import type { Connector, ConnectorType, SQLResult } from '../../connectors/interface.js';

// Mock dependencies
vi.mock('../../connectors/manager.js');
vi.mock('../../config/env.js');

// Mock connector for testing
const createMockConnector = (id: ConnectorType = 'sqlite'): Connector => ({
  id,
  name: 'Mock Connector',
  dsnParser: {} as any,
  connect: vi.fn(),
  disconnect: vi.fn(),
  getSchemas: vi.fn(),
  getTables: vi.fn(),
  tableExists: vi.fn(),
  getTableSchema: vi.fn(),
  getTableIndexes: vi.fn(),
  getStoredProcedures: vi.fn(),
  getStoredProcedureDetail: vi.fn(),
  executeSQL: vi.fn(),
});

// Helper function to parse tool response
const parseToolResponse = (response: any) => {
  return JSON.parse(response.content[0].text);
};

describe('execute-sql tool', () => {
  let mockConnector: Connector;
  const mockGetCurrentConnector = vi.mocked(ConnectorManager.getCurrentConnector);
  const mockIsReadOnlyMode = vi.mocked(isReadOnlyMode);

  beforeEach(() => {
    mockConnector = createMockConnector('sqlite');
    mockGetCurrentConnector.mockReturnValue(mockConnector);
    mockIsReadOnlyMode.mockReturnValue(false);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('single statement execution', () => {
    it('should execute a single SELECT statement successfully', async () => {
      const mockResult: SQLResult = { rows: [{ id: 1, name: 'test' }] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const result = await executeSqlToolHandler({ sql: 'SELECT * FROM users' }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(parsedResult.data.rows).toEqual([{ id: 1, name: 'test' }]);
      expect(parsedResult.data.count).toBe(1);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith('SELECT * FROM users');
    });

    it('should handle execution errors', async () => {
      vi.mocked(mockConnector.executeSQL).mockRejectedValue(new Error('Database error'));

      const result = await executeSqlToolHandler({ sql: 'SELECT * FROM invalid_table' }, null);

      expect(result.isError).toBe(true);
      const parsedResult = parseToolResponse(result);
      expect(parsedResult.success).toBe(false);
      expect(parsedResult.error).toBe('Database error');
      expect(parsedResult.code).toBe('EXECUTION_ERROR');
    });
  });

  describe('multi-statement execution', () => {
    it('should execute multiple SELECT statements', async () => {
      const mockResult: SQLResult = { 
        rows: [
          { id: 1, name: 'user1' },
          { id: 1, title: 'admin' }
        ] 
      };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const sql = 'SELECT * FROM users; SELECT * FROM roles;';
      const result = await executeSqlToolHandler({ sql }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(parsedResult.data.rows).toEqual([
        { id: 1, name: 'user1' },
        { id: 1, title: 'admin' }
      ]);
      expect(parsedResult.data.count).toBe(2);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith(sql);
    });

    it('should handle mixed read/write statements', async () => {
      const mockResult: SQLResult = { 
        rows: [{ id: 1, name: 'test_user' }] 
      };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const sql = "INSERT INTO users (name) VALUES ('test_user'); SELECT * FROM users WHERE name = 'test_user';";
      const result = await executeSqlToolHandler({ sql }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(parsedResult.data.rows).toEqual([{ id: 1, name: 'test_user' }]);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith(sql);
    });
  });

  describe('read-only mode validation', () => {
    beforeEach(() => {
      mockIsReadOnlyMode.mockReturnValue(true);
    });

    it('should allow single SELECT statement in read-only mode', async () => {
      const mockResult: SQLResult = { rows: [{ id: 1 }] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const result = await executeSqlToolHandler({ sql: 'SELECT * FROM users' }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalled();
    });

    it('should allow multiple SELECT statements in read-only mode', async () => {
      const mockResult: SQLResult = { rows: [] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const sql = 'SELECT * FROM users; SELECT * FROM roles; EXPLAIN SELECT * FROM users;';
      const result = await executeSqlToolHandler({ sql }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith(sql);
    });

    it('should reject single INSERT statement in read-only mode', async () => {
      const result = await executeSqlToolHandler({ sql: "INSERT INTO users (name) VALUES ('test')" }, null);

      expect(result.isError).toBe(true);
      const parsedResult = parseToolResponse(result);
      expect(parsedResult.success).toBe(false);
      expect(parsedResult.error).toContain('Read-only mode is enabled');
      expect(parsedResult.code).toBe('READONLY_VIOLATION');
      expect(mockConnector.executeSQL).not.toHaveBeenCalled();
    });

    it('should reject multi-statement with any write operation in read-only mode', async () => {
      const sql = "SELECT * FROM users; INSERT INTO users (name) VALUES ('test'); SELECT COUNT(*) FROM users;";
      const result = await executeSqlToolHandler({ sql }, null);

      expect(result.isError).toBe(true);
      const parsedResult = parseToolResponse(result);
      expect(parsedResult.success).toBe(false);
      expect(parsedResult.error).toContain('Read-only mode is enabled');
      expect(parsedResult.code).toBe('READONLY_VIOLATION');
      expect(mockConnector.executeSQL).not.toHaveBeenCalled();
    });

    it('should handle different database types allowed keywords', async () => {
      // Test PostgreSQL
      mockConnector = createMockConnector('postgres');
      mockGetCurrentConnector.mockReturnValue(mockConnector);
      
      const mockResult: SQLResult = { rows: [] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const result = await executeSqlToolHandler({ sql: 'SHOW TABLES; ANALYZE SELECT * FROM users;' }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalled();
    });
  });

  describe('database-specific behavior', () => {
    it('should work with SQLite allowed keywords', async () => {
      mockConnector = createMockConnector('sqlite');
      mockGetCurrentConnector.mockReturnValue(mockConnector);
      
      const mockResult: SQLResult = { rows: [] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const sql = 'SELECT * FROM users; PRAGMA table_info(users); EXPLAIN SELECT * FROM users;';
      const result = await executeSqlToolHandler({ sql }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith(sql);
    });

    it('should work with MySQL allowed keywords', async () => {
      mockConnector = createMockConnector('mysql');
      mockGetCurrentConnector.mockReturnValue(mockConnector);
      
      const mockResult: SQLResult = { rows: [] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const sql = 'SELECT * FROM users; DESCRIBE users; SHOW TABLES;';
      const result = await executeSqlToolHandler({ sql }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith(sql);
    });
  });

  describe('edge cases', () => {
    it('should handle empty SQL string', async () => {
      const mockResult: SQLResult = { rows: [] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const result = await executeSqlToolHandler({ sql: '' }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith('');
    });

    it('should handle SQL with only semicolons and whitespace', async () => {
      const mockResult: SQLResult = { rows: [] };
      vi.mocked(mockConnector.executeSQL).mockResolvedValue(mockResult);

      const result = await executeSqlToolHandler({ sql: '   ;  ;  ; ' }, null);
      const parsedResult = parseToolResponse(result);

      expect(parsedResult.success).toBe(true);
      expect(mockConnector.executeSQL).toHaveBeenCalledWith('   ;  ;  ; ');
    });
  });
});