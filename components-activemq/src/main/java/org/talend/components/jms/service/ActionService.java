// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jms.service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import java.util.Collections;

import static org.talend.components.jms.MessageConst.MESSAGE_CONTENT;

@Service
public class ActionService {

    public static final String ACTION_BASIC_HEALTH_CHECK = "ACTION_BASIC_HEALTH_CHECK";

    public static final String DISCOVER_SCHEMA = "discoverSchema";

    @Service
    private JmsService jmsService;

    @Service
    private I18nMessage i18n;

    @DiscoverSchema(DISCOVER_SCHEMA)
    public Schema guessSchema(BasicConfiguration config) {
        return new Schema(Collections.singletonList(new Schema.Entry(MESSAGE_CONTENT, Type.STRING)));
    }

    @HealthCheck(ACTION_BASIC_HEALTH_CHECK)
    public HealthCheckStatus validateBasicDatastore(@Option final JmsDataStore datastore) {
        if (datastore.getUrl() == null || datastore.getUrl().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyURL());
        }

        Connection connection = null;

        // create ConnectionFactory
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(datastore.getUrl());

        try {
            connection = jmsService.getConnection(connectionFactory, datastore.isUserIdentity(), datastore.getUserName(),
                    datastore.getPassword());
        } catch (JMSException e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.errorInvalidConnection());
        } finally {
            jmsService.closeConnection(connection);
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

}
