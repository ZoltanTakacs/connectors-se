package org.talend.components.activemq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.activemq.configuration.BasicConfiguration;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.components.activemq.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.activemq.testutils.JmsTestConstants.LOCALHOST;
import static org.talend.components.activemq.testutils.JmsTestConstants.PORT;

@WithComponents("org.talend.components.activemq")
class ActionServiceTest {

    @Service
    private ActionService actionService;

    @Test
    public void testJMSNoConnection() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setHost(LOCALHOST);
        dataStore.setPort(PORT);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertEquals("Invalid connection", status.getComment());
    }

    @Test
    public void testGuessSchema() {
        assertTrue(actionService.guessSchema(new BasicConfiguration()).getEntries()
                .contains(new Schema.Entry(MESSAGE_CONTENT, Type.STRING)));
    }

}
