import { defineConfig } from 'tsup';
import fs from 'fs';
import path from 'path';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  dts: true,
  clean: true,
  outDir: 'dist',
  // Copy the employee-sqlite resources to dist
  async onSuccess() {
    // Create target directory
    const targetDir = path.join('dist', 'resources', 'employee-sqlite');
    fs.mkdirSync(targetDir, { recursive: true });

    // Copy all SQL files from resources/employee-sqlite to dist/resources/employee-sqlite
    const sourceDir = path.join('resources', 'employee-sqlite');
    const files = fs.readdirSync(sourceDir);

    for (const file of files) {
      if (file.endsWith('.sql')) {
        const sourcePath = path.join(sourceDir, file);
        const targetPath = path.join(targetDir, file);
        fs.copyFileSync(sourcePath, targetPath);
        console.log(`Copied ${sourcePath} to ${targetPath}`);
      }
    }
  },
});
