package org.talend.components.jdbc.Output;

import org.apache.avro.Schema;
import org.talend.components.jdbc.dataset.QueryDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.components.jdbc.service.ActionService;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.service.Service;

import javax.sql.DataSource;
import java.sql.*;

public class JDBCOutputConfigService {

    @Service
    private JdbcService jdbcDriversService;

    @Service
    private ActionService actionService;

    JDBCOutputConfig configuration;



    public void updateSchema(JDBCOutputConfig configuration) {
        try {
            configuration.setSchema(getSchemaFromQuery(configuration));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Schema getSchemaFromQuery(JDBCOutputConfig configuration) throws Exception {
        final BasicDatastore basicDatastore = configuration.getDataset().getConnection();
        final QueryDataset dataset = configuration.getDataset();
        try (Connection conn = actionService.getConnection(basicDatastore);
             Statement statement = conn.createStatement();
             ResultSet resultset = statement.executeQuery(dataset.getSqlQuery())) {
            ResultSetMetaData metadata = resultset.getMetaData();
            return JDBCAvroRegistryString.get().inferSchema(metadata);
        }
    }


}
