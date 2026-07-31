/**
 * AWS Athena Connector Implementation
 *
 * Athena is serverless: there is no persistent connection or session. Each
 * executeSQL call is a StartQueryExecution → poll GetQueryExecution →
 * GetQueryResults round trip against the AWS API.
 *
 * DSN format: athena://{region}/{database}?workgroup=primary&output_location=s3://bucket/prefix&catalog=AwsDataCatalog
 * - region (host position) is required
 * - database (path) is required; it scopes unqualified table names and schema discovery
 * - workgroup, output_location, catalog are optional (output_location may instead
 *   come from the workgroup's server-side configuration)
 *
 * Credentials deliberately never appear in the DSN. The AWS SDK's default
 * credential provider chain is used (env vars, shared config/SSO profile,
 * instance/container role).
 */

import type {
  AthenaClient as AthenaClientType,
  ColumnInfo,
  Row,
} from "@aws-sdk/client-athena";
import {
  Connector,
  ConnectorType,
  ConnectorRegistry,
  DSNParser,
  SQLResult,
  SQLResultSet,
  TableColumn,
  TableIndex,
  StoredProcedure,
  ExecuteOptions,
  ConnectorConfig,
} from "../interface.js";
import { SafeURL } from "../../utils/safe-url.js";
import { obfuscateDSNPassword } from "../../utils/dsn-obfuscate.js";
import { SQLRowLimiter } from "../../utils/sql-row-limiter.js";
import { splitSQLStatements } from "../../utils/sql-parser.js";
import { isReadOnlySQL } from "../../utils/allowed-keywords.js";

interface AthenaConnectionOptions {
  region: string;
  database: string;
  workgroup?: string;
  outputLocation?: string;
  catalog?: string;
}

/** How long to wait for a query before cancelling it, when the source config sets no query_timeout. */
const DEFAULT_QUERY_TIMEOUT_SECONDS = 300;

class AthenaDSNParser implements DSNParser {
  async parse(dsn: string, _config?: ConnectorConfig): Promise<AthenaConnectionOptions> {
    if (!this.isValidDSN(dsn)) {
      throw new Error(
        `Invalid Athena DSN format.\nProvided: ${obfuscateDSNPassword(dsn)}\nExpected: ${this.getSampleDSN()}`
      );
    }

    const url = new SafeURL(dsn);
    const region = url.hostname;
    const database = decodeURIComponent(url.pathname.replace(/^\//, ""));

    if (!region) {
      throw new Error(`Athena DSN must include a region as the host, e.g. ${this.getSampleDSN()}`);
    }
    if (!database) {
      throw new Error(`Athena DSN must include a database in the path, e.g. ${this.getSampleDSN()}`);
    }

    return {
      region,
      database,
      workgroup: url.getSearchParam("workgroup") ?? undefined,
      outputLocation: url.getSearchParam("output_location") ?? undefined,
      catalog: url.getSearchParam("catalog") ?? undefined,
    };
  }

  getSampleDSN(): string {
    return "athena://us-east-1/default?workgroup=primary&output_location=s3://my-bucket/athena-results/";
  }

  isValidDSN(dsn: string): boolean {
    return dsn.startsWith("athena://");
  }
}

/** Escape a value as an Athena (Trino) single-quoted string literal. */
function escapeStringLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/**
 * Render a JS parameter value as the SQL literal Athena's ExecutionParameters
 * expects (the API substitutes each entry verbatim for a `?` placeholder).
 */
function toSQLLiteral(value: any): string {
  if (value === null || value === undefined) {
    return "NULL";
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (value instanceof Date) {
    return `TIMESTAMP ${escapeStringLiteral(value.toISOString().replace("T", " ").replace("Z", ""))}`;
  }
  return escapeStringLiteral(String(value));
}

const INTEGER_TYPES = new Set(["tinyint", "smallint", "integer", "int", "bigint"]);
const FLOAT_TYPES = new Set(["double", "float", "real", "decimal"]);

/** Convert Athena's string-typed Datum values to JS types based on column metadata. */
function convertValue(raw: string | undefined, columnType: string | undefined): any {
  if (raw === undefined) {
    return null;
  }
  const type = columnType?.toLowerCase() ?? "";
  if (INTEGER_TYPES.has(type) || FLOAT_TYPES.has(type)) {
    const parsed = Number(raw);
    return Number.isNaN(parsed) ? raw : parsed;
  }
  if (type === "boolean") {
    return raw === "true";
  }
  return raw;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class AthenaConnector implements Connector {
  id: ConnectorType = "athena";
  name = "AWS Athena";
  dsnParser = new AthenaDSNParser();

  private client: AthenaClientType | null = null;
  private options: AthenaConnectionOptions | null = null;
  private queryTimeoutSeconds = DEFAULT_QUERY_TIMEOUT_SECONDS;

  // Source ID is set by ConnectorManager after cloning
  private sourceId: string = "default";

  getId(): string {
    return this.sourceId;
  }

  clone(): Connector {
    return new AthenaConnector();
  }

  async connect(dsn: string, initScript?: string, config?: ConnectorConfig): Promise<void> {
    this.options = await this.dsnParser.parse(dsn, config);
    if (config?.queryTimeoutSeconds) {
      this.queryTimeoutSeconds = config.queryTimeoutSeconds;
    }

    // Lazy-import so the SDK stays an optional dependency (see loadConnectors).
    const { AthenaClient } = await import("@aws-sdk/client-athena");
    // No validation round trip: Athena has no connection to establish, and any
    // probe API call would require IAM permissions a query-only principal may
    // lack. Credential/config errors surface on the first query instead.
    this.client = new AthenaClient({ region: this.options.region });

    if (initScript) {
      await this.executeSQL(initScript, {});
    }
  }

  async disconnect(): Promise<void> {
    this.client?.destroy();
    this.client = null;
  }

  private required(): { client: AthenaClientType; options: AthenaConnectionOptions } {
    if (!this.client || !this.options) {
      throw new Error("Not connected to Athena");
    }
    return { client: this.client, options: this.options };
  }

  /**
   * Execute one statement end to end: start it, poll until it finishes (or the
   * query timeout passes, in which case it is cancelled server-side), then page
   * through the results.
   */
  private async runQuery(sql: string, parameters?: any[]): Promise<SQLResultSet> {
    const { client, options } = this.required();
    const {
      StartQueryExecutionCommand,
      GetQueryExecutionCommand,
      GetQueryResultsCommand,
      StopQueryExecutionCommand,
    } = await import("@aws-sdk/client-athena");

    const started = await client.send(
      new StartQueryExecutionCommand({
        QueryString: sql,
        QueryExecutionContext: {
          Database: options.database,
          ...(options.catalog ? { Catalog: options.catalog } : {}),
        },
        ...(options.workgroup ? { WorkGroup: options.workgroup } : {}),
        ...(options.outputLocation
          ? { ResultConfiguration: { OutputLocation: options.outputLocation } }
          : {}),
        ...(parameters && parameters.length > 0
          ? { ExecutionParameters: parameters.map(toSQLLiteral) }
          : {}),
      })
    );
    const queryExecutionId = started.QueryExecutionId!;

    const deadline = Date.now() + this.queryTimeoutSeconds * 1000;
    let pollDelayMs = 200;
    for (;;) {
      const execution = await client.send(
        new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId })
      );
      const status = execution.QueryExecution?.Status;
      const state = status?.State;
      if (state === "SUCCEEDED") {
        break;
      }
      if (state === "FAILED" || state === "CANCELLED") {
        throw new Error(
          `Athena query ${state.toLowerCase()}: ${status?.StateChangeReason ?? "no reason given"}`
        );
      }
      if (Date.now() >= deadline) {
        try {
          await client.send(new StopQueryExecutionCommand({ QueryExecutionId: queryExecutionId }));
        } catch {
          // best effort - the timeout error below is the one that matters
        }
        throw new Error(
          `Athena query timed out after ${this.queryTimeoutSeconds}s and was cancelled (execution id ${queryExecutionId})`
        );
      }
      await sleep(pollDelayMs);
      pollDelayMs = Math.min(pollDelayMs * 2, 2000);
    }

    // Page through results. The row count for write statements (INSERT/CTAS)
    // comes back as UpdateCount instead of rows.
    const rows: any[] = [];
    let updateCount: number | undefined;
    let columns: ColumnInfo[] = [];
    let nextToken: string | undefined;
    let firstPage = true;
    do {
      const page = await client.send(
        new GetQueryResultsCommand({
          QueryExecutionId: queryExecutionId,
          ...(nextToken ? { NextToken: nextToken } : {}),
        })
      );
      if (page.UpdateCount !== undefined) {
        updateCount = page.UpdateCount;
      }
      const pageRows = page.ResultSet?.Rows ?? [];
      let dataRows = pageRows;
      if (firstPage) {
        columns = page.ResultSet?.ResultSetMetadata?.ColumnInfo ?? [];
        // Athena repeats the column labels as the first data row of SELECT
        // results (but not for DDL like SHOW/DESCRIBE), so skip the first row
        // exactly when it mirrors the column labels.
        if (pageRows.length > 0 && this.isHeaderRow(pageRows[0], columns)) {
          dataRows = pageRows.slice(1);
        }
        firstPage = false;
      }
      for (const row of dataRows) {
        rows.push(this.toRowObject(row, columns));
      }
      nextToken = page.NextToken;
    } while (nextToken);

    return { sql, rows, rowCount: rows.length > 0 ? rows.length : (updateCount ?? 0) };
  }

  private isHeaderRow(row: Row, columns: ColumnInfo[]): boolean {
    const data = row.Data ?? [];
    return (
      columns.length > 0 &&
      data.length === columns.length &&
      data.every((datum, i) => datum.VarCharValue === columns[i].Label)
    );
  }

  private toRowObject(row: Row, columns: ColumnInfo[]): Record<string, any> {
    const result: Record<string, any> = {};
    (row.Data ?? []).forEach((datum, i) => {
      const column = columns[i];
      result[column?.Label ?? `column_${i}`] = convertValue(datum.VarCharValue, column?.Type);
    });
    return result;
  }

  /** Run an introspection query and return its rows. */
  private async query(sql: string): Promise<Record<string, any>[]> {
    return (await this.runQuery(sql)).rows;
  }

  async getSchemas(): Promise<string[]> {
    const rows = await this.query(
      "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
    );
    return rows.map((row) => row.schema_name);
  }

  async getDefaultSchema(): Promise<string | null> {
    return this.required().options.database;
  }

  private async getRelations(kind: "BASE TABLE" | "VIEW", schema?: string): Promise<string[]> {
    const targetSchema = schema ?? this.required().options.database;
    const rows = await this.query(
      `SELECT table_name FROM information_schema.tables ` +
        `WHERE table_schema = ${escapeStringLiteral(targetSchema)} ` +
        `AND table_type = '${kind}' ORDER BY table_name`
    );
    return rows.map((row) => row.table_name);
  }

  async getTables(schema?: string): Promise<string[]> {
    return this.getRelations("BASE TABLE", schema);
  }

  async getViews(schema?: string): Promise<string[]> {
    return this.getRelations("VIEW", schema);
  }

  async tableExists(tableName: string, schema?: string): Promise<boolean> {
    const targetSchema = schema ?? this.required().options.database;
    const rows = await this.query(
      `SELECT 1 AS present FROM information_schema.tables ` +
        `WHERE table_schema = ${escapeStringLiteral(targetSchema)} ` +
        `AND table_name = ${escapeStringLiteral(tableName)} LIMIT 1`
    );
    return rows.length > 0;
  }

  async getTableSchema(tableName: string, schema?: string): Promise<TableColumn[]> {
    const targetSchema = schema ?? this.required().options.database;
    const rows = await this.query(
      `SELECT column_name, data_type, is_nullable, column_default, comment ` +
        `FROM information_schema.columns ` +
        `WHERE table_schema = ${escapeStringLiteral(targetSchema)} ` +
        `AND table_name = ${escapeStringLiteral(tableName)} ORDER BY ordinal_position`
    );
    return rows.map((row) => ({
      column_name: row.column_name,
      data_type: row.data_type,
      is_nullable: row.is_nullable,
      column_default: row.column_default ?? null,
      description: row.comment || null,
    }));
  }

  async getTableIndexes(_tableName: string, _schema?: string): Promise<TableIndex[]> {
    // Athena scans data files in S3; there are no indexes to report.
    return [];
  }

  async getStoredProcedures(_schema?: string, _routineType?: "procedure" | "function"): Promise<string[]> {
    // Athena has no stored procedures (UDFs are Lambda-backed and not introspectable via SQL).
    return [];
  }

  async getStoredProcedureDetail(_procedureName: string, _schema?: string): Promise<StoredProcedure> {
    throw new Error("Athena does not support stored procedures.");
  }

  async executeSQL(sql: string, options: ExecuteOptions, parameters?: any[]): Promise<SQLResult> {
    this.required();
    const statements = splitSQLStatements(sql, "athena");

    if (parameters && parameters.length > 0 && statements.length > 1) {
      throw new Error("Parameters are not supported for multi-statement queries in Athena");
    }

    const resultSets: SQLResultSet[] = [];
    for (const statement of statements) {
      // Athena has no engine-level read-only mode, so the keyword classifier
      // is re-checked here as the connector's backstop (the execute_sql tool
      // already gates on the same classifier before calling us).
      if (options.readonly && !isReadOnlySQL(statement, "athena")) {
        throw new Error(`Read-only mode is enabled. Statement rejected: ${statement}`);
      }
      const processed = SQLRowLimiter.applyMaxRows(statement, options.maxRows);
      resultSets.push(await this.runQuery(processed, parameters));
    }
    return { resultSets };
  }
}

// Register the Athena connector
const athenaConnector = new AthenaConnector();
ConnectorRegistry.register(athenaConnector);
