package org.talend.components.jdbc.Output;


import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.talend.components.jdbc.DriverInfo;
import org.talend.components.jdbc.dataset.QueryDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.service.Service;

import java.util.List;

@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCOutput")
@Documentation("This component writes data to database.")
@Processor(name = "JDBCOutput")
public class JDBCOutput extends PTransform<PCollection<IndexedRecord>, PDone> {
    @Service
    private JDBCOutputConfigService service;

    @Service
    private JdbcService jdbcDriversService;

    private final JDBCOutputConfig configuration;


    public JDBCOutput(@Option("Configuration") final JDBCOutputConfig config) {
        configuration = config;
        service.updateSchema(configuration);
    }

    @Override
    public PDone expand(final PCollection<IndexedRecord> input) {
        final BasicDatastore basicDatastore = configuration.getDataset().getConnection();
        final QueryDataset dataset = configuration.getDataset();

        DriverInfo driverInfo = jdbcDriversService.getDrivers().get(basicDatastore.getDbType());
        return input.apply(JdbcIO.<IndexedRecord> write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                .create(driverInfo.getClazz(),
                        basicDatastore.getJdbcUrl())
                .withUsername(basicDatastore.getUserId())
                .withPassword(basicDatastore.getPassword()))
                .withStatement(
                        JDBCSQLBuilder.getInstance().generateSQL4Insert(dataset.getTableName(),
                                configuration.getSchema()))
                .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<IndexedRecord>) (indexedRecord, preparedStatement) -> {
                    List<Schema.Field> fields = indexedRecord.getSchema().getFields();
                    int index = 0;
                    for (Schema.Field f : fields) {
                        JDBCMapping.setValue(++index, preparedStatement, f, indexedRecord.get(f.pos()));
                    }
                }));
    }
}
