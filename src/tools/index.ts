import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { executeSqlToolHandler, executeSqlSchema } from "./execute-sql.js";
/**
 * Register all tool handlers with the MCP server
 */
export function registerTools(server: McpServer): void {
  // Tool to run a SQL query (read-only for safety)
  server.tool(
    "execute_sql",
    "Execute a SQL query on the current database",
    executeSqlSchema,
    executeSqlToolHandler
  );

}
