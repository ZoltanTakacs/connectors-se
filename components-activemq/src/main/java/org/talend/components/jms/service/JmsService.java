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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.talend.components.jms.configuration.MessageType;
import org.talend.sdk.component.api.service.Service;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JmsService {

    @Service
    private I18nMessage i18n;

    public Destination getDestination(Session session, String destination, MessageType messageType) throws JMSException {
        return (MessageType.QUEUE == messageType) ? session.createQueue(destination) : session.createTopic(destination);
    }

    public Session getSession(Connection connection) throws JMSException {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public Connection getConnection(ConnectionFactory connectionFactory, boolean isUserIdentity, String userName, String password)
            throws JMSException {
        return isUserIdentity ? connectionFactory.createConnection(userName, password) : connectionFactory.createConnection();
    }

    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
    }

    public void closeSession(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                log.warn(i18n.warnSessionCantBeClosed(), e);
            }
        }
    }

    public void closeProducer(MessageProducer producer) {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException e) {
                log.warn(i18n.warnProducerCantBeClosed(), e);
            }
        }
    }

    public void closeConsumer(MessageConsumer consumer) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConsumerCantBeClosed(), e);
            }
        }
    }

}
