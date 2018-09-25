package org.talend.components.activemq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.activemq.configuration.Broker;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.activemq.testutils.JmsTestConstants.LOCALHOST;
import static org.talend.components.activemq.testutils.JmsTestConstants.PORT;

@WithComponents("org.talend.components.activemq")
class ActionServiceTestIT {

    @Service
    private ActionService actionService;

    @Test
    public void testJMSSuccessfulConnection() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setHost(LOCALHOST);
        dataStore.setPort(PORT);
        dataStore.setUseSSL(true);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    public void testJMSSuccessfulConnectionStaticDiscovery() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setStaticDiscovery(true);
        dataStore.setUseSSL(true);
        List<Broker> brokerList = new ArrayList<>();
        Broker broker1 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        Broker broker2 = new Broker();
        broker2.setHost(LOCALHOST);
        broker2.setPort(PORT);
        brokerList.add(broker1);
        brokerList.add(broker2);
        dataStore.setBrokers(brokerList);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    public void testJMSSuccessfulConnectionFailover() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setFailover(true);
        dataStore.setUseSSL(true);
        List<Broker> brokerList = new ArrayList<>();
        Broker broker1 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        Broker broker2 = new Broker();
        broker2.setHost(LOCALHOST);
        broker2.setPort(PORT);
        brokerList.add(broker1);
        brokerList.add(broker2);
        dataStore.setBrokers(brokerList);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

}
