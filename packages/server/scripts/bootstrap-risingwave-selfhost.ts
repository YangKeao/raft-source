#!/usr/bin/env -S node --import=@oxc-node/core/register
/**
 * One-shot, release-bound RisingWave bootstrap for self-hosted Raft.
 *
 * Unlike bootstrap-risingwave-local.ts this script accepts remote Kubernetes
 * services and never drops a publication, replication slot, source, or view.
 * It is intentionally CREATE-only: a partial failure requires an operator to
 * inspect and clean the dedicated Raft resources before retrying.
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

import {
  RISINGWAVE_ARTIFACT_CDC_TABLES,
  RISINGWAVE_BOOTSTRAP_ARTIFACTS,
  RISINGWAVE_CDC_TABLES,
  RISINGWAVE_LOCAL_SOURCE,
  RISINGWAVE_PUBLICATION_TABLES,
  RISINGWAVE_REQUIRED_RELATIONS,
  buildRisingWaveBootstrapStatements,
  createdRelationName,
} from "../../../scripts/dev/raftdev-risingwave-bootstrap";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "../../..");
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function connectionUrl(name: string): URL {
  const raw = requiredEnv(name);
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`${name} is not a valid URL`);
  }
  if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") {
    throw new Error(`${name} must use postgres:// or postgresql://`);
  }
  return url;
}

function sqlIdentifier(value: string): string {
  if (!/^[a-z_][a-z0-9_]*$/.test(value)) throw new Error(`unsafe SQL identifier: ${value}`);
  return `"${value}"`;
}

function sqlLiteral(value: string): string {
  if (value.includes("\0")) throw new Error("SQL values cannot contain NUL bytes");
  return `'${value.replaceAll("'", "''")}'`;
}

function databaseName(url: URL): string {
  const value = decodeURIComponent(url.pathname.replace(/^\//, ""));
  if (!value) throw new Error("DATABASE_URL must name a database");
  return value;
}

async function ensurePublication(
  postgres: pg.Client,
  publicationName: string,
  cdcRole: string,
): Promise<void> {
  const publication = await postgres.query<{ exists: boolean }>(
    "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1) AS exists",
    [publicationName],
  );
  const tableList = RISINGWAVE_PUBLICATION_TABLES
    .map((name) => `public.${sqlIdentifier(name)}`)
    .join(", ");
  if (!publication.rows[0]?.exists) {
    await postgres.query(
      `CREATE PUBLICATION ${sqlIdentifier(publicationName)} FOR TABLE ${tableList}`,
    );
  }

  const actualResult = await postgres.query<{ tablename: string }>(
    "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = 'public' ORDER BY tablename",
    [publicationName],
  );
  const actual = actualResult.rows.map((row) => row.tablename).sort();
  const expected = [...RISINGWAVE_PUBLICATION_TABLES].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`publication table set mismatch: expected ${expected.length}, got ${actual.length}`);
  }

  await postgres.query(`GRANT USAGE ON SCHEMA public TO ${sqlIdentifier(cdcRole)}`);
  await postgres.query(`GRANT SELECT ON TABLE ${tableList} TO ${sqlIdentifier(cdcRole)}`);
}

async function assertUnusedNames(
  postgres: pg.Client,
  risingwave: pg.Client,
  slotName: string,
  sourceName: string,
): Promise<void> {
  const slot = await postgres.query(
    "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
    [slotName],
  );
  if (slot.rowCount) {
    throw new Error(`replication slot ${slotName} already exists; refusing a destructive retry`);
  }
  const sources = await risingwave.query<{ name: string }>("SHOW SOURCES");
  if (sources.rows.some((row) => row.name === sourceName)) {
    throw new Error(`RisingWave source ${sourceName} already exists; refusing a destructive retry`);
  }
}

async function createCdcSource(
  risingwave: pg.Client,
  options: {
    sourceName: string;
    host: string;
    port: number;
    username: string;
    password: string;
    database: string;
    sslMode: string;
    slotName: string;
    publicationName: string;
  },
): Promise<void> {
  const sourceSql = [
    `CREATE SOURCE ${sqlIdentifier(options.sourceName)}`,
    "WITH (",
    "  connector = 'postgres-cdc',",
    `  hostname = ${sqlLiteral(options.host)},`,
    `  port = ${sqlLiteral(String(options.port))},`,
    `  username = ${sqlLiteral(options.username)},`,
    `  password = ${sqlLiteral(options.password)},`,
    `  database.name = ${sqlLiteral(options.database)},`,
    "  schema.name = 'public',",
    `  ssl.mode = ${sqlLiteral(options.sslMode)},`,
    `  slot.name = ${sqlLiteral(options.slotName)},`,
    `  publication.name = ${sqlLiteral(options.publicationName)},`,
    "  publication.create.enable = 'false'",
    ")",
  ].join("\n");
  await risingwave.query(sourceSql);
}

function rewriteSource(statement: string, sourceName: string): string {
  return statement.replaceAll(
    new RegExp(`\\b${RISINGWAVE_LOCAL_SOURCE}\\b`, "g"),
    sqlIdentifier(sourceName),
  );
}

async function waitForExactSourceCounts(
  postgres: pg.Client,
  risingwave: pg.Client,
  tables: readonly { upstream: string; name: string }[],
  timeoutMs: number,
): Promise<Record<string, number>> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() <= deadline) {
    const counts: Record<string, number> = {};
    const mismatches: string[] = [];
    for (const table of tables) {
      const pgResult = await postgres.query(
        `SELECT count(*)::text AS count FROM public.${sqlIdentifier(table.upstream)}`,
      );
      const rwResult = await risingwave.query(
        `SELECT count(*)::text AS count FROM ${sqlIdentifier(table.name)}`,
      );
      const pgCount = Number(pgResult.rows[0]?.count ?? -1);
      const rwCount = Number(rwResult.rows[0]?.count ?? -1);
      counts[table.name] = rwCount;
      if (pgCount !== rwCount) mismatches.push(`${table.name}:${rwCount}/${pgCount}`);
    }
    if (mismatches.length === 0) return counts;
    last = mismatches.join(", ");
    await sleep(1_000);
  }
  throw new Error(`CDC snapshot count parity timed out (${last || "no readable counts"})`);
}

async function assertRequiredRelations(risingwave: pg.Client): Promise<void> {
  const result = await risingwave.query<{ name: string }>(
    `SELECT name FROM rw_catalog.rw_relations WHERE name IN (${RISINGWAVE_REQUIRED_RELATIONS.map(sqlLiteral).join(", ")})`,
  );
  const actual = new Set(result.rows.map((row) => row.name));
  const missing = RISINGWAVE_REQUIRED_RELATIONS.filter((name) => !actual.has(name));
  if (missing.length) throw new Error(`RisingWave serving graph is missing: ${missing.join(", ")}`);

  const ddl = await risingwave.query<{ count: string }>(
    "SELECT count(*)::text AS count FROM rw_catalog.rw_ddl_progress",
  );
  if (Number(ddl.rows[0]?.count ?? -1) !== 0) {
    throw new Error("RisingWave still has background DDL in progress");
  }
}

async function main(): Promise<void> {
  if (process.env.RAFT_SELFHOST_RISINGWAVE_BOOTSTRAP !== "1") {
    throw new Error("refusing bootstrap without RAFT_SELFHOST_RISINGWAVE_BOOTSTRAP=1");
  }

  const postgresUrl = connectionUrl("DATABASE_URL");
  const risingwaveUrl = connectionUrl("RISINGWAVE_DATABASE_URL");
  const sourceName = process.env.RISINGWAVE_SOURCE_NAME?.trim() || "raft_pg_cdc";
  const publicationName = process.env.RISINGWAVE_PUBLICATION_NAME?.trim() || "raft_rw_publication";
  const slotName = process.env.RISINGWAVE_SLOT_NAME?.trim() || "raft_rw_slot";
  const cdcHost = requiredEnv("RISINGWAVE_CDC_HOST");
  const cdcUsername = requiredEnv("RISINGWAVE_CDC_USERNAME");
  const cdcPassword = requiredEnv("RISINGWAVE_CDC_PASSWORD");
  const cdcDatabase = process.env.RISINGWAVE_CDC_DATABASE?.trim() || databaseName(postgresUrl);
  const cdcPort = Number(process.env.RISINGWAVE_CDC_PORT || "5432");
  const sslMode = process.env.RISINGWAVE_CDC_SSL_MODE?.trim() || "prefer";
  const timeoutMs = Number(process.env.RISINGWAVE_BOOTSTRAP_TIMEOUT_MS || "600000");

  for (const value of [sourceName, publicationName, slotName, cdcUsername]) sqlIdentifier(value);
  if (!Number.isInteger(cdcPort) || cdcPort < 1 || cdcPort > 65535) {
    throw new Error("RISINGWAVE_CDC_PORT must be an integer between 1 and 65535");
  }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 30_000 || timeoutMs > 3_600_000) {
    throw new Error("RISINGWAVE_BOOTSTRAP_TIMEOUT_MS must be between 30000 and 3600000");
  }
  if (!new Set(["disable", "prefer", "require", "verify-ca", "verify-full"]).has(sslMode)) {
    throw new Error("RISINGWAVE_CDC_SSL_MODE is invalid");
  }

  const postgres = new pg.Client({ connectionString: postgresUrl.toString(), connectionTimeoutMillis: 15_000 });
  const risingwave = new pg.Client({ connectionString: risingwaveUrl.toString(), connectionTimeoutMillis: 15_000 });
  postgres.on("error", (error) => console.error("[rw-bootstrap] Postgres client error:", error.message));
  risingwave.on("error", (error) => console.error("[rw-bootstrap] RisingWave client error:", error.message));

  await postgres.connect();
  await risingwave.connect();
  try {
    const wal = await postgres.query<{ wal_level: string }>("SHOW wal_level");
    if (wal.rows[0]?.wal_level !== "logical") {
      throw new Error(`Postgres wal_level must be logical, got ${wal.rows[0]?.wal_level ?? "unknown"}`);
    }

    await ensurePublication(postgres, publicationName, cdcUsername);
    await assertUnusedNames(postgres, risingwave, slotName, sourceName);
    await createCdcSource(risingwave, {
      sourceName,
      host: cdcHost,
      port: cdcPort,
      username: cdcUsername,
      password: cdcPassword,
      database: cdcDatabase,
      sslMode,
      slotName,
      publicationName,
    });

    const statements = buildRisingWaveBootstrapStatements(Object.fromEntries(
      RISINGWAVE_BOOTSTRAP_ARTIFACTS.map((artifact) => [
        artifact.file,
        readFileSync(join(ROOT, artifact.file), "utf8"),
      ]),
    ));
    const pendingCdcRelations = new Set(RISINGWAVE_CDC_TABLES.map((table) => table.name));
    let sourceCounts: Record<string, number> | null = null;
    for (let index = 0; index < statements.length; index++) {
      const statement = rewriteSource(statements[index], sourceName);
      await risingwave.query(statement);
      const relation = createdRelationName(statement);
      if (relation) {
        console.error(`[rw-bootstrap] ${index + 1}/${statements.length} ${relation}`);
        pendingCdcRelations.delete(relation);
      }
      if (pendingCdcRelations.size === 0 && sourceCounts === null) {
        sourceCounts = await waitForExactSourceCounts(postgres, risingwave, RISINGWAVE_CDC_TABLES, timeoutMs);
      }
    }
    if (sourceCounts === null) throw new Error("bootstrap did not create every CDC relation");
    const artifactCounts = await waitForExactSourceCounts(
      postgres,
      risingwave,
      RISINGWAVE_ARTIFACT_CDC_TABLES,
      timeoutMs,
    );
    await assertRequiredRelations(risingwave);

    console.log(JSON.stringify({
      ok: true,
      source: sourceName,
      publication: publicationName,
      slot: slotName,
      publicationTableCount: RISINGWAVE_PUBLICATION_TABLES.length,
      relationCount: RISINGWAVE_REQUIRED_RELATIONS.length,
      sourceCounts: { ...sourceCounts, ...artifactCounts },
    }, null, 2));
  } finally {
    await Promise.allSettled([postgres.end(), risingwave.end()]);
  }
}

main().catch((error) => {
  let message = error instanceof Error ? error.message : String(error);
  for (const secret of [
    process.env.DATABASE_URL,
    process.env.RISINGWAVE_DATABASE_URL,
    process.env.RISINGWAVE_CDC_PASSWORD,
  ]) {
    if (secret) message = message.replaceAll(secret, "<redacted>");
  }
  console.error(`RisingWave self-host bootstrap failed: ${message}`);
  process.exit(1);
});
