package org.talend.components.jdbc.Output;

import lombok.Data;
import org.apache.avro.Schema;
import org.talend.components.jdbc.dataset.QueryDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

@Data
public class JDBCOutputConfig {

    @Option
    @Documentation("The dataset for JDBC")
    private QueryDataset dataset;

    @Option
    @Documentation("The schema for JDBC")
    private Schema schema;
}
