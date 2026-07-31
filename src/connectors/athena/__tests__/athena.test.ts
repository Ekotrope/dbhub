import { describe, it, expect, vi } from "vitest";
import { AthenaConnector } from "../index.js";
import { isReadOnlySQL } from "../../../utils/allowed-keywords.js";

describe("Athena DSN parser", () => {
  const parser = new AthenaConnector().dsnParser;

  it("parses region, database, and query params", async () => {
    const parsed = await parser.parse(
      "athena://us-east-1/analytics?workgroup=primary&output_location=s3://bucket/prefix/&catalog=AwsDataCatalog"
    );
    expect(parsed).toEqual({
      region: "us-east-1",
      database: "analytics",
      workgroup: "primary",
      outputLocation: "s3://bucket/prefix/",
      catalog: "AwsDataCatalog",
    });
  });

  it("rejects a DSN without a database", async () => {
    await expect(parser.parse("athena://us-east-1/")).rejects.toThrow(/database/);
  });

  it("rejects a non-athena DSN", () => {
    expect(parser.isValidDSN("postgres://host/db")).toBe(false);
    expect(parser.isValidDSN("athena://us-east-1/db")).toBe(true);
  });
});

describe("Athena read-only classification", () => {
  it("allows reads and rejects writes", () => {
    expect(isReadOnlySQL("SELECT * FROM t", "athena")).toBe(true);
    expect(isReadOnlySQL("SHOW TABLES", "athena")).toBe(true);
    expect(isReadOnlySQL("DESCRIBE t", "athena")).toBe(true);
    expect(isReadOnlySQL("INSERT INTO t VALUES (1)", "athena")).toBe(false);
    expect(isReadOnlySQL("CREATE TABLE t AS SELECT 1", "athena")).toBe(false);
    expect(isReadOnlySQL("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", "athena")).toBe(false);
  });
});

/**
 * Wire a connector to a fake AthenaClient that walks one query through
 * StartQueryExecution → GetQueryExecution (SUCCEEDED) → GetQueryResults.
 */
function connectorWithFakeClient(resultsPage: any): AthenaConnector {
  const connector = new AthenaConnector();
  const send = vi.fn(async (command: any) => {
    switch (command.constructor.name) {
      case "StartQueryExecutionCommand":
        return { QueryExecutionId: "qid-1" };
      case "GetQueryExecutionCommand":
        return { QueryExecution: { Status: { State: "SUCCEEDED" } } };
      case "GetQueryResultsCommand":
        return resultsPage;
      default:
        throw new Error(`Unexpected command: ${command.constructor.name}`);
    }
  });
  (connector as any).client = { send, destroy: () => {} };
  (connector as any).options = { region: "us-east-1", database: "analytics" };
  return connector;
}

describe("Athena executeSQL", () => {
  it("skips the header row and converts values by column type", async () => {
    const connector = connectorWithFakeClient({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [
            { Label: "id", Type: "bigint" },
            { Label: "name", Type: "varchar" },
            { Label: "active", Type: "boolean" },
          ],
        },
        Rows: [
          { Data: [{ VarCharValue: "id" }, { VarCharValue: "name" }, { VarCharValue: "active" }] },
          { Data: [{ VarCharValue: "42" }, { VarCharValue: "alice" }, { VarCharValue: "true" }] },
          { Data: [{ VarCharValue: "7" }, {}, { VarCharValue: "false" }] },
        ],
      },
    });

    const result = await connector.executeSQL("SELECT id, name, active FROM users", {});
    expect(result.resultSets).toHaveLength(1);
    expect(result.resultSets[0].rows).toEqual([
      { id: 42, name: "alice", active: true },
      { id: 7, name: null, active: false },
    ]);
    expect(result.resultSets[0].rowCount).toBe(2);
  });

  it("rejects writes when readonly, before any AWS call", async () => {
    const connector = connectorWithFakeClient({});
    await expect(
      connector.executeSQL("DROP TABLE users", { readonly: true })
    ).rejects.toThrow(/Read-only mode/);
    expect((connector as any).client.send).not.toHaveBeenCalled();
  });

  it("applies maxRows as a LIMIT clause", async () => {
    const connector = connectorWithFakeClient({ ResultSet: { Rows: [] } });
    await connector.executeSQL("SELECT * FROM users", { maxRows: 10 });
    const startCommand = (connector as any).client.send.mock.calls[0][0];
    expect(startCommand.input.QueryString).toMatch(/LIMIT 10/);
  });
});
