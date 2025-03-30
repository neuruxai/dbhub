import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { runQueryToolHandler, runQuerySchema } from './run-query.js';
import { listConnectorsToolHandler, listConnectorsSchema } from './list-connectors.js';

/**
 * Register all tool handlers with the MCP server
 */
export function registerTools(server: McpServer): void {
  // Tool to run a SQL query (read-only for safety)
  server.tool(
    "run_query",
    runQuerySchema,
    runQueryToolHandler
  );

  // Tool to list available database connectors
  server.tool(
    "list_connectors",
    listConnectorsSchema,
    listConnectorsToolHandler
  );
}