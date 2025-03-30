import { z } from "zod";
import { ConnectorManager } from '../connectors/manager.js';
import { ConnectorRegistry } from '../connectors/interface.js';
import { createToolSuccessResponse } from '../utils/response-formatter.js';
import { isDemoMode } from '../config/env.js';

// Empty schema for list_connectors tool to prevent protocol validation errors
export const listConnectorsSchema = {};

/**
 * list_connectors tool handler
 * Lists all available database connectors and their sample DSNs
 * Indicates which connector is active based on current DSN
 */
export async function listConnectorsToolHandler(_args: {} | undefined, _extra: any) {
  const samples = ConnectorRegistry.getAllSampleDSNs();
  
  // Get active connector if possible
  let activeConnectorId: string | null = null;
  try {
    // Check if we have an active connection using static method
    const activeConnector = ConnectorManager.getCurrentConnector();
    activeConnectorId = activeConnector.id;
  } catch (error) {
    // No active connector yet or not connected
  }
  
  // If we're in demo mode, SQLite should be active
  const isDemo = isDemoMode();
  if (isDemo && !activeConnectorId) {
    activeConnectorId = 'sqlite';
  }
  
  // Convert to a more structured format
  const sampleObjects = Object.entries(samples).map(([id, dsn]) => ({ 
    id, 
    dsn,
    active: id === activeConnectorId
  }));
  
  // Prepare response data
  const responseData = {
    connectors: sampleObjects,
    count: sampleObjects.length,
    activeConnector: activeConnectorId,
    demoMode: isDemo
  };
  
  // Use the utility to create a standardized response
  return createToolSuccessResponse(responseData);
}