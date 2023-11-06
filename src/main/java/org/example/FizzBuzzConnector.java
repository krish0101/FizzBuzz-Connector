package org.example;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class FizzBuzzConnector implements Connector, FizzBuzzConnectorInterface {

    private static String catalogName = null;
    private static String schemaName = null;
    private final String tableName;

    public FizzBuzzConnector(String catalogName, String schemaName, String tableName)

    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new FizzBuzzConnectorMetadata() {
            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
                return null;
            }

            @Override
            public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
                return null;
            }

            @Override
            public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
                return null;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
                return null;
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
                return null;
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
                return null;
            }

            @Override
            public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
                return null;
            }
        };
    }

    @Override
    public RecordCursor getCursor(ConnectorSession session, ConnectorSplit split) {
        return new FizzBuzzRecordCursor(session);
    }

    @Override
    public List<ConnectorSplit> getSplits(ConnectorSession session, SchemaTableName tableName) {
        return ImmutableList.of(new ConnectorSplit() {
            @Override
            public NodeSelectionStrategy getNodeSelectionStrategy() {
                return null;
            }

            @Override
            public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
                return null;
            }

            @Override
            public Object getInfo() {
                return null;
            }
        });
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return null;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return null;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return null;
    }

    @Override
    public void shutdown() {
    }

    private static class FizzBuzzConnectorMetadata implements ConnectorMetadata, org.example.FizzBuzzConnectorMetadata {

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return ImmutableList.of((String.valueOf(new SchemaTableName(catalogName, schemaName))));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return null;
        }

        @Override
        public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
            return null;
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return null;
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
            return null;
        }

        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName) {
            return new TableMetadata(
                    catalogName,
                    schemaName,
                    tableName,
                    ImmutableList.of(
                            new com.facebook.presto.spi.ColumnMetadata("id", VarcharType.VARCHAR),
                            new com.facebook.presto.spi.ColumnMetadata("FizzBuzz", VarcharType.VARCHAR)
                    )
            );
        }
    }

    private static class FizzBuzzRecordCursor implements RecordCursor, org.example.FizzBuzzRecordCursor {

        private final ConnectorSession session;
        private int currentRow = 0;

        public FizzBuzzRecordCursor(ConnectorSession session) {
            this.session = session;
        }

        @Override
        public long getReadBytes() {
            return 0;
        }

        @Override
        public long getCompletedBytes() {
            return 0;
        }

        @Override
        public long getReadTimeNanos() {
            return 0;
        }

        @Override
        public Type getType(int field) {
            return null;
        }

        @Override
        public boolean advanceNextPosition() {
            return currentRow < 10001;
        }

        @Override
        public boolean getBoolean(int field) {
            return false;
        }

        @Override
        public long getLong(int field) {
            return 0;
        }

        @Override
        public double getDouble(int field) {
            return 0;
        }

        @Override
        public Slice getSlice(int field) {
            return null;
        }

        @Override
        public Object getObject(int field) {
            return null;
        }

        @Override
        public boolean isNull(int field) {
            return false;
        }

        @Override
        public Object[] getFields() {
            return new Object[]{currentRow, getFizzBuzz(currentRow++)};
        }

        private String getFizzBuzz(int i) {
            if (i % 3 == 0 && i % 5 == 0) {
                return "FizzBuzz";
            }
            else if (i % 3 == 0) {
                return "Fizz";
            }
            else if (i % 5 == 0) {
                return "Buzz";
            }
            else {
                return String.valueOf(i);
            }
        }

        @Override
        public void close() {
        }
    }
}

/* Creating Presto catalog:
CREATE CATALOG fizzbuzz;

Creating Presto schema:
CREATE SCHEMA fizzbuzz.default;

Creating FizzBuzz table:
CREATE TABLE fizzbuzz.default.fizzbuzz (
    id INTEGER,
    FizzBuzz STRING
);

Executing the FizzBuzz program:
java -cp presto-client-latest.jar com.facebook.presto.client.QueryRunner fizzbuzz:default

This starts a Presto client that connects to the Presto server. The SQL queries to interact with the FizzBuzz table are as follows.

Printing the entire FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz;

Printing all Fizz entries in the FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz WHERE FizzBuzz = 'Fizz';

Printing all Buzz entries in the FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz WHERE FizzBuzz = 'Buzz';

Printing all FizzBuzz entries in the FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz WHERE FizzBuzz = 'FizzBuzz'; */