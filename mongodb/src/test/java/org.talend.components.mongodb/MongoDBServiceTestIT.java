package org.talend.components.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

@Disabled("Need connect to MongoDB")
@WithComponents("org.talend.components.mongodb")
public class MongoDBServiceTestIT {

    MongoDBDataStore datastore;

    MongoDBService service;

    @Service
    private Messages i18n;

    @BeforeEach
    public void before() {
        datastore = new MongoDBDataStore();
        datastore.setServer("localhost");
        datastore.setPort("27017");
        datastore.setDatabase("testdb");
        service = new MongoDBService();
    }

    @Test
    public void testGetConnection() {
        // null Authentication
        MongoClient mongo = service.getConnection(datastore);
        Assert.assertNotNull(mongo);

    }

    @Test
    public void getConnection() {
        // with auth
        datastore.setAuthentication(true);
        datastore.setAuthentication_Databse("test");
        datastore.setUsername("pyzhou");
        datastore.setPassword("talend");
        datastore.setAuthentication_mechanism(MongoDBDataStore.Authentication_method.SCRAMSHA1_MEC);
        MongoClient mongo = service.getConnection(datastore);
        Assert.assertNotNull(mongo);
    }

    @Test
    public void getCollection() {
        MongoDBInputDataset dataset = new MongoDBInputDataset();
        dataset.setDataStore(datastore);
        dataset.setCollection("personalstakesTEST651_12");
        dataset.setQuery("{}");
        service.testConnection(datastore, i18n);
        MongoCollection<Document> collection = service.getCollection(dataset);
        Assert.assertNotNull(collection);

    }

    @AfterEach
    public void close() {
        service.close();
    }

    @Test
    public void testConnection() {
        HealthCheckStatus healthCheckStatus = service.testConnection(datastore, i18n);
        Assert.assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }

    @Test
    public void testConnection2() {
        datastore.setServer("localhost2");
        HealthCheckStatus healthCheckStatus = service.testConnection(datastore, i18n);
        Assert.assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());
    }

}
