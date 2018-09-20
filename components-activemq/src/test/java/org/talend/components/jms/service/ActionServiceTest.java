package org.talend.components.jms.service;

import org.junit.jupiter.api.Test;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.jms.testutils.JmsTestConstants.URL;

@WithComponents("org.talend.components.jms")
class ActionServiceTest {

    @Service
    private ActionService actionService;

    @Test
    public void testJMSNoConnection() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setUrl(URL);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertEquals("Invalid connection", status.getComment());
    }

    @Test
    public void testGuessSchema() {
        assertTrue(actionService.guessSchema(new BasicConfiguration()).getEntries()
                .contains(new Schema.Entry(MESSAGE_CONTENT, Type.STRING)));
    }

    @Test
    public void testJMSConnectionEmptyUrl() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setUrl("");
        assertThrows(IllegalArgumentException.class, () -> {
            actionService.validateBasicDatastore(dataStore);
        });
    }

}
