import { Connector, SQLResult, TableColumn, TableIndex, ConnectorRegistry, StoredProcedure, ConnectorType } from '../interface.js';
import oracledb, { Connection, ExecuteManyOptions, ExecuteOptions, Pool, Result, BindParameters } from 'oracledb';
import { allowedKeywords } from '../../utils/allowed-keywords.js';
import { SafeURL } from '../../utils/safe-url.js';
import { obfuscateDSNPassword } from '../../utils/dsn-obfuscate.js';

// Adjust output format for large numbers and dates if needed
// oracledb.fetchAsString = [ oracledb.NUMBER, oracledb.DATE, oracledb.TIMESTAMP_TZ ];

// Potentially increase the size of the Node.js thread pool if needed
// process.env.UV_THREADPOOL_SIZE = "10";

// Configure Oracle to return JavaScript values
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

// Oracle result row type interfaces
interface SchemaRow {
  SCHEMA_NAME: string;
}
interface TableRow {
  TABLE_NAME: string;
}
interface ColumnRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  IS_NULLABLE: string;
  COLUMN_DEFAULT: string | null;
}
interface IndexRow {
  INDEX_NAME: string;
  UNIQUENESS: string;
}
interface IndexColumnRow {
  COLUMN_NAME: string;
}
interface CountRow {
  COUNT: number;
}
interface SchemaInfoRow {
  SCHEMA: string;
}
interface ProcedureRow {
  OBJECT_NAME: string;
}
interface ProcedureTypeRow {
  OBJECT_TYPE: string;
}
interface SourceRow {
  TEXT: string;
}
interface ArgumentRow {
  ARGUMENT_NAME: string;
  IN_OUT: string;
  DATA_TYPE: string;
  DATA_LENGTH?: number;
  DATA_PRECISION?: number;
  DATA_SCALE?: number;
}
interface PrimaryKeyRow {
  IS_PK: number;
}

export class OracleConnector implements Connector {
  // Connector ID and Name are part of the Connector interface
  id: ConnectorType = 'oracle';
  name: string = 'Oracle Database';

  private pool: Pool | null = null;
  private currentSchema: string | null = null;

  // constructor(config: ConnectionConfig) { // Removed config
  constructor() {
    // Set auto-commit to true for simpler transaction handling
    oracledb.autoCommit = true;
  }

  // Oracle DSN Parser implementation
  dsnParser = {
    parse: async (dsn: string): Promise<oracledb.PoolAttributes> => {
      if (!this.dsnParser.isValidDSN(dsn)) {
        const obfuscatedDSN = obfuscateDSNPassword(dsn);
        const expectedFormat = this.dsnParser.getSampleDSN();
        throw new Error(
          `Invalid Oracle DSN format.\nProvided: ${obfuscatedDSN}\nExpected: ${expectedFormat}`
        );
      }

      try {
        const url = new SafeURL(dsn);

        // Extract service name from pathname (remove leading slash)
        let serviceName = url.pathname;
        if (serviceName.startsWith('/')) {
          serviceName = serviceName.substring(1);
        }

        // Construct the connectString in Oracle format
        const port = url.port ? parseInt(url.port) : 1521; // Default Oracle port is 1521
        const connectString = `${url.hostname}:${port}/${serviceName}`;
        
        // Set up the connection config
        const config: oracledb.PoolAttributes = {
          user: url.username,
          password: url.password,
          connectString: connectString,
          poolMin: 0,
          poolMax: 10,
          poolIncrement: 1,
        };
        
        // Handle additional options from query parameters
        url.forEachSearchParam((value, key) => {
          switch (key.toLowerCase()) {
            case 'poolmin':
              config.poolMin = parseInt(value, 10);
              break;
            case 'poolmax':
              config.poolMax = parseInt(value, 10);
              break;
            case 'poolincrement':
              config.poolIncrement = parseInt(value, 10);
              break;
            case 'sslmode':
              switch (value.toLowerCase()) {
                case 'disable':
                  // No SSL
                  break;
                case 'require':
                  // SSL without verification
                  config.sslServerDNMatch = false;
                  break;
              }
              break;
          }
        });
        
        return config;
      } catch (error) {
        throw new Error(`Failed to parse Oracle DSN: ${error instanceof Error ? error.message : String(error)}`);
      }
    },

    getSampleDSN: (): string => {
      return 'oracle://username:password@host:1521/service_name?sslmode=require';
    },

    isValidDSN: (dsn: string): boolean => {
      try {
        return dsn.startsWith('oracle://');
      } catch (error) {
        return false;
      }
    },
  };

  // Track if we've already initialized the client
  private static clientInitialized = false;

  // Initialize Oracle client only once
  private initClient(): void {
    if (OracleConnector.clientInitialized) {
      return;
    }

    try {
      if (process.env.ORACLE_LIB_DIR) {
        oracledb.initOracleClient({ libDir: process.env.ORACLE_LIB_DIR });
        console.error('Oracle client initialized in Thick mode');
      } else {
        console.error('ORACLE_LIB_DIR not specified, will use Thin mode by default');
      }
      OracleConnector.clientInitialized = true;
    } catch (err) {
      console.error('Failed to initialize Oracle client:', err);
      // We'll continue with Thin mode, but it might not work with all server versions
    }
  }

  async connect(dsn: string, initializationScript?: string): Promise<void> {
    try {
      // Initialize Oracle client settings when connecting to Oracle database
      this.initClient();

      const config = await this.dsnParser.parse(dsn);

      // Create a connection pool
      this.pool = await oracledb.createPool(config);

      // Get a connection to test and determine current schema
      const conn = await this.getConnection();
      try {
        const result = await conn.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') as SCHEMA FROM DUAL");
        if (result.rows && result.rows.length > 0) {
          this.currentSchema = (result.rows[0] as SchemaInfoRow).SCHEMA;
        }

        // Run initialization script if provided
        if (initializationScript) {
          await conn.execute(initializationScript);
        }
      } finally {
        await conn.close();
      }

      console.error('Successfully connected to Oracle database');
      if (this.currentSchema) {
        console.error(`Current schema: ${this.currentSchema}`);
      }
    } catch (error) {
      console.error('Failed to connect to Oracle database:', error);
      
      // Add more helpful error message for NJS-138 error
      if (error instanceof Error && error.message.includes('NJS-138')) {
        const enhancedError = new Error(
          `${error.message}\n\nThis error occurs when your Oracle database version is not supported by node-oracledb in Thin mode.\n` +
          `To resolve this, you need to use Thick mode:\n` +
          `1. Download Oracle Instant Client from https://www.oracle.com/database/technologies/instant-client/downloads.html\n` +
          `2. Set ORACLE_LIB_DIR environment variable to the path of your Oracle Instant Client\n` +
          `   Example: export ORACLE_LIB_DIR=/path/to/instantclient_19_8\n` +
          `3. Restart DBHub`
        );
        throw enhancedError;
      }
      
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      try {
        await this.pool.close();
        this.pool = null;
        this.currentSchema = null;
      } catch (error) {
        console.error('Error disconnecting from Oracle:', error);
        throw error;
      }
    }
  }

  async getSchemas(): Promise<string[]> {
    try {
      const conn = await this.getConnection();
      try {
        // Query all schemas (users) that the current user has access to
        const result = await conn.execute(
          `SELECT USERNAME AS SCHEMA_NAME 
           FROM ALL_USERS 
           ORDER BY USERNAME`
        );

        return result.rows?.map((row) => (row as SchemaRow).SCHEMA_NAME) || [];
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error getting schemas from Oracle:', error);
      throw error;
    }
  }

  async getTables(schemaName?: string): Promise<string[]> {
    try {
      const conn = await this.getConnection();
      try {
        const schema = schemaName || this.currentSchema;

        const result = await conn.execute(
          `SELECT TABLE_NAME 
           FROM ALL_TABLES 
           WHERE OWNER = :schema
           ORDER BY TABLE_NAME`,
          { schema: schema?.toUpperCase() }
        );

        return result.rows?.map((row) => (row as TableRow).TABLE_NAME) || [];
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error getting tables from Oracle:', error);
      throw error;
    }
  }

  async getTableColumns(tableName: string, schemaName?: string): Promise<TableColumn[]> {
    try {
      const conn = await this.getConnection();
      try {
        const schema = schemaName || this.currentSchema;

        const result = await conn.execute(
          `SELECT 
             COLUMN_NAME, 
             DATA_TYPE,
             NULLABLE as IS_NULLABLE,
             DATA_DEFAULT as COLUMN_DEFAULT
           FROM ALL_TAB_COLUMNS
           WHERE OWNER = :schema
           AND TABLE_NAME = :tableName
           ORDER BY COLUMN_ID`,
          {
            schema: schema?.toUpperCase(),
            tableName: tableName.toUpperCase(),
          }
        );

        return (
          result.rows?.map((row) => ({
            column_name: (row as ColumnRow).COLUMN_NAME,
            data_type: (row as ColumnRow).DATA_TYPE,
            is_nullable: (row as ColumnRow).IS_NULLABLE === 'Y' ? 'YES' : 'NO',
            column_default: (row as ColumnRow).COLUMN_DEFAULT,
          })) || []
        );
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error getting columns from Oracle:', error);
      throw error;
    }
  }

  // Method to ensure boolean return type
  private ensureBoolean(value: boolean | undefined): boolean {
    return value === true;
  }

  async getTableIndexes(tableName: string, schemaName?: string): Promise<TableIndex[]> {
    try {
      const conn = await this.getConnection();
      try {
        const schema = schemaName || this.currentSchema;

        // First, get all indexes for the table
        const indexesResult = await conn.execute(
          `SELECT 
             i.INDEX_NAME,
             i.UNIQUENESS
           FROM ALL_INDEXES i
           WHERE i.OWNER = :schema
           AND i.TABLE_NAME = :tableName`,
          {
            schema: schema?.toUpperCase(),
            tableName: tableName.toUpperCase(),
          }
        );

        if (!indexesResult.rows || indexesResult.rows.length === 0) {
          return [];
        }

        const indexes: TableIndex[] = [];

        // For each index, get its columns
        for (const idx of indexesResult.rows) {
          const indexRow = idx as IndexRow;
          const indexName = indexRow.INDEX_NAME;
          const isUnique = indexRow.UNIQUENESS === 'UNIQUE';

          const columnsResult = await conn.execute(
            `SELECT 
               COLUMN_NAME
             FROM ALL_IND_COLUMNS
             WHERE INDEX_OWNER = :schema
             AND INDEX_NAME = :indexName
             ORDER BY COLUMN_POSITION`,
            {
              schema: schema?.toUpperCase(),
              indexName: indexName,
            }
          );

          const columnNames = columnsResult.rows?.map((row) => (row as IndexColumnRow).COLUMN_NAME) || [];

          // Check if this is a primary key
          const pkResult = await conn.execute(
            `SELECT COUNT(*) AS IS_PK
             FROM ALL_CONSTRAINTS
             WHERE CONSTRAINT_TYPE = 'P'
             AND OWNER = :schema
             AND TABLE_NAME = :tableName
             AND INDEX_NAME = :indexName`,
            {
              schema: schema?.toUpperCase(),
              tableName: tableName.toUpperCase(),
              indexName: indexName,
            }
          );

          const isPrimary = pkResult.rows && pkResult.rows.length > 0 && (pkResult.rows[0] as PrimaryKeyRow).IS_PK > 0;

          indexes.push({
            index_name: indexName,
            column_names: columnNames,
            is_unique: isUnique,
            is_primary: !!isPrimary, // Ensure boolean
          });
        }

        return indexes;
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error getting indexes from Oracle:', error);
      throw error;
    }
  }

  async tableExists(tableName: string, schemaName?: string): Promise<boolean> {
    try {
      const conn = await this.getConnection();
      try {
        const schema = schemaName || this.currentSchema;

        const result = await conn.execute(
          `SELECT COUNT(*) AS COUNT
           FROM ALL_TABLES
           WHERE OWNER = :schema
           AND TABLE_NAME = :tableName`,
          {
            schema: schema?.toUpperCase(),
            tableName: tableName.toUpperCase(),
          }
        );

        // Ensure we return a boolean
        return !!(result.rows && result.rows.length > 0 && (result.rows[0] as CountRow).COUNT > 0);
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error checking table existence in Oracle:', error);
      throw error;
    }
  }

  async getTableSchema(tableName: string, schema?: string | undefined): Promise<TableColumn[]> {
    // This seems redundant with getTableColumns, delegate for now
    return this.getTableColumns(tableName, schema);
  }

  async getStoredProcedures(schema?: string): Promise<string[]> {
    try {
      const conn = await this.getConnection();
      try {
        const schemaName = schema || this.currentSchema;

        const result = await conn.execute(
          `SELECT OBJECT_NAME
           FROM ALL_OBJECTS
           WHERE OWNER = :schema
           AND OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
           ORDER BY OBJECT_NAME`,
          { schema: schemaName?.toUpperCase() }
        );

        return result.rows?.map((row) => (row as ProcedureRow).OBJECT_NAME) || [];
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error getting stored procedures from Oracle:', error);
      throw error;
    }
  }

  async getStoredProcedureDetail(procedureName: string, schema?: string): Promise<StoredProcedure> {
    try {
      const conn = await this.getConnection();
      try {
        const schemaName = schema || this.currentSchema;

        // Get procedure type (PROCEDURE or FUNCTION)
        const typeResult = await conn.execute(
          `SELECT OBJECT_TYPE
           FROM ALL_OBJECTS
           WHERE OWNER = :schema
           AND OBJECT_NAME = :procName`,
          {
            schema: schemaName?.toUpperCase(),
            procName: procedureName.toUpperCase(),
          }
        );

        if (!typeResult.rows || typeResult.rows.length === 0) {
          throw new Error(`Procedure or function ${procedureName} not found`);
        }

        const objectType = (typeResult.rows[0] as ProcedureTypeRow).OBJECT_TYPE;
        const isProcedure = objectType === 'PROCEDURE';

        // Get procedure text (source code)
        const sourceResult = await conn.execute(
          `SELECT TEXT
           FROM ALL_SOURCE
           WHERE OWNER = :schema
           AND NAME = :procName
           AND TYPE = :objectType
           ORDER BY LINE`,
          {
            schema: schemaName?.toUpperCase(),
            procName: procedureName.toUpperCase(),
            objectType,
          }
        );

        let definition = '';
        if (sourceResult.rows && sourceResult.rows.length > 0) {
          definition = sourceResult.rows.map((row) => (row as SourceRow).TEXT).join('');
        }

        // Get parameters
        const paramsResult = await conn.execute(
          `SELECT 
             ARGUMENT_NAME,
             IN_OUT,
             DATA_TYPE,
             DATA_LENGTH,
             DATA_PRECISION,
             DATA_SCALE
           FROM ALL_ARGUMENTS
           WHERE OWNER = :schema
           AND OBJECT_NAME = :procName
           AND POSITION > 0
           ORDER BY SEQUENCE`,
          {
            schema: schemaName?.toUpperCase(),
            procName: procedureName.toUpperCase(),
          }
        );

        let parameterList = '';
        let returnType = '';

        if (paramsResult.rows && paramsResult.rows.length > 0) {
          const params = paramsResult.rows
            .map((row) => {
              const argRow = row as ArgumentRow;
              if (argRow.IN_OUT === 'OUT' && !isProcedure) {
                // For functions, the return value is marked as an OUT parameter
                returnType = formatOracleDataType(
                  argRow.DATA_TYPE,
                  argRow.DATA_LENGTH,
                  argRow.DATA_PRECISION,
                  argRow.DATA_SCALE
                );
                return null;
              }

              const paramType = formatOracleDataType(
                argRow.DATA_TYPE,
                argRow.DATA_LENGTH,
                argRow.DATA_PRECISION,
                argRow.DATA_SCALE
              );

              return `${argRow.ARGUMENT_NAME} ${argRow.IN_OUT} ${paramType}`;
            })
            .filter(Boolean);

          parameterList = params.join(', ');
        }

        return {
          procedure_name: procedureName,
          procedure_type: isProcedure ? 'procedure' : 'function',
          language: 'PL/SQL',
          parameter_list: parameterList,
          return_type: returnType || undefined,
          definition: definition || undefined,
        };
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error getting stored procedure details from Oracle:', error);
      throw error;
    }
  }

  async executeSQL(sql: string, params?: any[]): Promise<SQLResult> {
    try {
      const conn = await this.getConnection();
      try {
        // Transform parameters to named binding format if provided
        let bindParams: any = undefined;
        if (params && params.length > 0) {
          bindParams = {};
          // Oracle uses named parameters like :param1, :param2
          // We'll transform array parameters to this format
          for (let i = 0; i < params.length; i++) {
            bindParams[`param${i + 1}`] = params[i];
          }

          // Replace ? with named parameters in SQL
          let paramIndex = 1;
          sql = sql.replace(/\?/g, () => `:param${paramIndex++}`);
        }

        const options = {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          autoCommit: true,
        };

        // Validation is now handled in the execute-sql.ts tool handler

        const result = await conn.execute(sql, bindParams || {}, options);

        return {
          rows: result.rows || [],
          rowCount: result.rows?.length || 0,
          fields:
            result.metaData?.map((col) => ({
              name: col.name,
              type: col.dbType?.toString() || 'UNKNOWN',
            })) || [],
        };
      } finally {
        await conn.close();
      }
    } catch (error) {
      console.error('Error executing query in Oracle:', error);
      throw error;
    }
  }

  // Helper method to get a connection from the pool
  private async getConnection(): Promise<Connection> {
    if (!this.pool) {
      throw new Error('Connection pool not initialized. Call connect() first.');
    }
    return this.pool.getConnection();
  }
}

// Helper function to format Oracle data types
function formatOracleDataType(
  dataType: string,
  dataLength?: number,
  dataPrecision?: number,
  dataScale?: number
): string {
  if (!dataType) {
    return 'UNKNOWN';
  }

  switch (dataType.toUpperCase()) {
    case 'VARCHAR2':
    case 'CHAR':
    case 'NVARCHAR2':
    case 'NCHAR':
      return `${dataType}(${dataLength || ''})`;
    case 'NUMBER':
      if (dataPrecision !== undefined && dataScale !== undefined) {
        return `NUMBER(${dataPrecision}, ${dataScale})`;
      } else if (dataPrecision !== undefined) {
        return `NUMBER(${dataPrecision})`;
      }
      return 'NUMBER';
    default:
      return dataType;
  }
}

// Register the connector
ConnectorRegistry.register(new OracleConnector());
