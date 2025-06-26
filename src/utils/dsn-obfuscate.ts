/**
 * Obfuscates the password in a DSN string for logging purposes
 * @param dsn The original DSN string
 * @returns DSN string with password replaced by asterisks
 */
export function obfuscateDSNPassword(dsn: string): string {
  if (!dsn) {
    return dsn;
  }

  try {
    // Handle different DSN formats
    const protocolMatch = dsn.match(/^([^:]+):/);
    if (!protocolMatch) {
      return dsn; // Not a recognizable DSN format
    }

    const protocol = protocolMatch[1];

    // For SQLite file paths, don't obfuscate
    if (protocol === 'sqlite') {
      return dsn;
    }

    // For other databases, look for password pattern: ://user:password@host
    // We need to be careful with @ in passwords, so we'll find the last @ that separates password from host
    const protocolPart = dsn.split('://')[1];
    if (!protocolPart) {
      return dsn;
    }
    
    // Find the last @ to separate credentials from host
    const lastAtIndex = protocolPart.lastIndexOf('@');
    if (lastAtIndex === -1) {
      return dsn; // No @ found, no password to obfuscate
    }
    
    const credentialsPart = protocolPart.substring(0, lastAtIndex);
    const hostPart = protocolPart.substring(lastAtIndex + 1);
    
    // Check if there's a colon in credentials (user:password format)
    const colonIndex = credentialsPart.indexOf(':');
    if (colonIndex === -1) {
      return dsn; // No colon found, no password to obfuscate
    }
    
    const username = credentialsPart.substring(0, colonIndex);
    const password = credentialsPart.substring(colonIndex + 1);
    const obfuscatedPassword = '*'.repeat(Math.min(password.length, 8));
    
    return `${protocol}://${username}:${obfuscatedPassword}@${hostPart}`;
  } catch (error) {
    // If any error occurs during obfuscation, return the original DSN
    // This ensures we don't break functionality due to obfuscation issues
    return dsn;
  }
}