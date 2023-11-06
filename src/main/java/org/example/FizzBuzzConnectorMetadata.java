package org.example;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;

public interface FizzBuzzConnectorMetadata {
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName);
}
