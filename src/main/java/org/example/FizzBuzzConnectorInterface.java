package org.example;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;

import java.util.List;

public interface FizzBuzzConnectorInterface {
    ConnectorMetadata getMetadata();

    RecordCursor getCursor(ConnectorSession session, ConnectorSplit split);

    List<ConnectorSplit> getSplits(ConnectorSession session, SchemaTableName tableName);
}
