import dotenv from "dotenv";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

// Create __dirname equivalent for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Parse command line arguments
export function parseCommandLineArgs() {
  // Check if any args start with '--' (the way tsx passes them)
  const args = process.argv.slice(2);
  const parsedManually: Record<string, string> = {};

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith("--")) {
      const [key, value] = arg.substring(2).split("=");
      if (value) {
        // Handle --key=value format
        parsedManually[key] = value;
      } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        // Handle --key value format
        parsedManually[key] = args[i + 1];
        i++; // Skip the next argument as it's the value
      } else {
        // Handle --key format (boolean flag)
        parsedManually[key] = "true";
      }
    }
  }

  // Just use the manually parsed args - removed parseArgs dependency for Node.js <18.3.0 compatibility
  return parsedManually;
}

/**
 * Load environment files from various locations
 * Returns the name of the file that was loaded, or null if none was found
 */
export function loadEnvFiles(): string | null {
  // Determine if we're in development or production mode
  const isDevelopment = process.env.NODE_ENV === "development" || process.argv[1]?.includes("tsx");

  // Select environment file names based on environment
  const envFileNames = isDevelopment
    ? [".env.local", ".env"] // In development, try .env.local first, then .env
    : [".env"]; // In production, only look for .env

  // Build paths to check for environment files
  const envPaths = [];
  for (const fileName of envFileNames) {
    envPaths.push(
      fileName, // Current working directory
      path.join(__dirname, "..", "..", fileName), // Two levels up (src/config -> src -> root)
      path.join(process.cwd(), fileName) // Explicit current working directory
    );
  }

  // Try to load the first env file found from the prioritized locations
  for (const envPath of envPaths) {
    console.error(`Checking for env file: ${envPath}`);
    if (fs.existsSync(envPath)) {
      dotenv.config({ path: envPath });
      // Return the name of the file that was loaded
      return path.basename(envPath);
    }
  }

  return null;
}

/**
 * Check if demo mode is enabled from command line args
 * Returns true if --demo flag is provided
 */
export function isDemoMode(): boolean {
  const args = parseCommandLineArgs();
  return args.demo === "true";
}

/**
 * Check if readonly mode is enabled from command line args or environment
 * Returns true if --readonly flag is provided
 */
export function isReadOnlyMode(): boolean {
  const args = parseCommandLineArgs();
  
  // Check command line args first
  if (args.readonly !== undefined) {
    return args.readonly === "true";
  }
  
  // Check environment variable
  if (process.env.READONLY !== undefined) {
    return process.env.READONLY === "true";
  }
  
  // Default to false
  return false;
}

/**
 * Resolve DSN from command line args, environment variables, or .env files
 * Returns the DSN and its source, or null if not found
 */
export function resolveDSN(): { dsn: string; source: string; isDemo?: boolean } | null {
  // Get command line arguments
  const args = parseCommandLineArgs();

  // Check for demo mode first (highest priority)
  if (isDemoMode()) {
    // Will use in-memory SQLite with demo data
    return {
      dsn: "sqlite:///:memory:",
      source: "demo mode",
      isDemo: true,
    };
  }

  // 1. Check command line arguments
  if (args.dsn) {
    return { dsn: args.dsn, source: "command line argument" };
  }

  // 2. Check environment variables before loading .env
  if (process.env.DSN) {
    return { dsn: process.env.DSN, source: "environment variable" };
  }

  // 3. Try loading from .env files
  const loadedEnvFile = loadEnvFiles();
  if (loadedEnvFile && process.env.DSN) {
    return { dsn: process.env.DSN, source: `${loadedEnvFile} file` };
  }

  return null;
}

/**
 * Resolve transport type from command line args or environment variables
 * Returns 'stdio' or 'http' (streamable HTTP), with 'stdio' as the default
 */
export function resolveTransport(): { type: "stdio" | "http"; source: string } {
  // Get command line arguments
  const args = parseCommandLineArgs();

  // 1. Check command line arguments first (highest priority)
  if (args.transport) {
    const type = args.transport === "http" ? "http" : "stdio";
    return { type, source: "command line argument" };
  }

  // 2. Check environment variables
  if (process.env.TRANSPORT) {
    const type = process.env.TRANSPORT === "http" ? "http" : "stdio";
    return { type, source: "environment variable" };
  }

  // 3. Default to stdio
  return { type: "stdio", source: "default" };
}

/**
 * Resolve port from command line args or environment variables
 * Returns port number with 8080 as the default
 *
 * Note: The port option is only applicable when using --transport=http
 * as it controls the HTTP server port for streamable HTTP connections.
 */
export function resolvePort(): { port: number; source: string } {
  // Get command line arguments
  const args = parseCommandLineArgs();

  // 1. Check command line arguments first (highest priority)
  if (args.port) {
    const port = parseInt(args.port, 10);
    return { port, source: "command line argument" };
  }

  // 2. Check environment variables
  if (process.env.PORT) {
    const port = parseInt(process.env.PORT, 10);
    return { port, source: "environment variable" };
  }

  // 3. Default to 8080
  return { port: 8080, source: "default" };
}

/**
 * Redact sensitive information from a DSN string
 * Replaces the password with asterisks
 * @param dsn - The DSN string to redact
 * @returns The sanitized DSN string
 */
export function redactDSN(dsn: string): string {
  try {
    // Create a URL object to parse the DSN
    const url = new URL(dsn);

    // Replace the password with asterisks
    if (url.password) {
      url.password = "*******";
    }

    // Return the sanitized DSN
    return url.toString();
  } catch (error) {
    // If parsing fails, do basic redaction with regex
    return dsn.replace(/\/\/([^:]+):([^@]+)@/, "//$1:***@");
  }
}
