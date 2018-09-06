package org.talend.components.netsuite.runtime;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.ApiVersion;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.LoginType;
import org.talend.components.netsuite.runtime.client.MetaDataSource;
import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteCredentials;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NetSuiteVersion;
import org.talend.components.netsuite.runtime.client.NsTokenPassport;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Represents NetSuite Web Service endpoint.
 */

public class NetSuiteEndpoint {

    public static final String CONNECTION = "NetSuite_Connection";

    /** Creates instance of NetSuite client. */
    private NetSuiteClientFactory<?> clientFactory;

    /** Connection configuration for this endpoint. */
    private ConnectionConfig connectionConfig;

    /** NetSuite client. */
    private NetSuiteClientService<?> clientService;

    /**
     * Creates new instance using given client factory and connection configuration.
     *
     * @param clientFactory client factory
     * @param connectionConfig connection configuration
     */
    public NetSuiteEndpoint(NetSuiteClientFactory<?> clientFactory, ConnectionConfig connectionConfig) {
        this.clientFactory = clientFactory;
        this.connectionConfig = connectionConfig;
    }

    /**
     * Create connection configuration for given connection properties.
     *
     * @param properties connection properties
     * @return connection configuration
     * @throws NetSuiteException if connection configuration not valid
     */
    public static ConnectionConfig createConnectionConfig(NetsuiteDataStore properties) throws NetSuiteException {
        if (StringUtils.isEmpty(properties.getEndpoint())) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.endpointUrlRequired"));
            throw new RuntimeException();
        }
        if (StringUtils.isEmpty(properties.getAccount())) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.accountRequired"));
            throw new RuntimeException();
        }

        NetSuiteCredentials credentials = null;
        NsTokenPassport tokenPassport = null;
        if (properties.getLoginType() == LoginType.BASIC) {

            if (StringUtils.isEmpty(properties.getEmail())) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.emailRequired"));
                throw new RuntimeException();
            }
            if (StringUtils.isEmpty(properties.getPassword())) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.passwordRequired"));
                throw new RuntimeException();
            }

            if (properties.getRole() == 0) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.roleRequired"));
                throw new RuntimeException();
            }

            credentials = new NetSuiteCredentials(properties.getEmail(), properties.getPassword(), properties.getAccount(),
                    String.valueOf(properties.getRole()), properties.getApplicationId());
        } else {
            if (StringUtils.isEmpty(properties.getConsumerKey())) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.passwordRequired"));
                throw new RuntimeException();
            }
            if (StringUtils.isEmpty(properties.getConsumerSecret())) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.passwordRequired"));
                throw new RuntimeException();
            }
            if (StringUtils.isEmpty(properties.getTokenId())) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.passwordRequired"));
                throw new RuntimeException();
            }
            if (StringUtils.isEmpty(properties.getTokenSecret())) {
                // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.passwordRequired"));
                throw new RuntimeException();
            }
            tokenPassport = new NsTokenPassport(properties.getAccount(), properties.getConsumerKey(),
                    properties.getConsumerSecret(), properties.getTokenId(), properties.getTokenSecret());
        }

        NetSuiteVersion endpointApiVersion;
        try {
            endpointApiVersion = NetSuiteVersion.detectVersion(properties.getEndpoint());
        } catch (IllegalArgumentException e) {
            // TODO: Exception
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.couldNotDetectApiVersionFromEndpointUrl",
            // endpointUrl));
            throw new RuntimeException();
        }
        ApiVersion apiVersionString = properties.getApiVersion();
        NetSuiteVersion apiVersion;
        try {
            apiVersion = NetSuiteVersion.parseVersion(apiVersionString);
        } catch (IllegalArgumentException e) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.invalidApiVersion", apiVersionString));
            throw new RuntimeException();
        }

        if (!endpointApiVersion.isSameMajor(apiVersion)) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.endpointUrlApiVersionMismatch", endpointUrl,
            // apiVersionString));
            throw new RuntimeException();
        }

        ConnectionConfig connectionConfig = new ConnectionConfig(properties.getEndpoint(), apiVersion.getMajor(), credentials,
                tokenPassport, properties.isEnableCustomization());
        // connectionConfig.setReferenceComponentId(properties.getReferencedComponentId());
        // No shared connection in tacokit.
        return connectionConfig;
    }

    /**
     * Connect to NetSuite remote endpoint.
     *
     * @return NetSuite client
     * @throws NetSuiteException if an error occurs during connecting
     */
    public NetSuiteClientService<?> connect() throws NetSuiteException {
        clientService = connect(connectionConfig);

        return clientService;
    }

    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    /**
     * Return NetSuite client.
     *
     * <p>
     * If endpoint is not yet connected then the method creates client and
     * connects ({@link #connect()}) to NetSuite.
     *
     * @return client
     * @throws NetSuiteException if an error occurs during connecting
     */
    public NetSuiteClientService<?> getClientService() throws NetSuiteException {
        return clientService == null ? connect() : clientService;
    }

    /**
     * Creates new NetSuite client and connects to NetSuite remote endpoint.
     *
     * @param connectionConfig connection configuration
     * @return client
     * @throws NetSuiteException if an error occurs during connecting
     */
    private NetSuiteClientService<?> connect(ConnectionConfig connectionConfig) throws NetSuiteException {

        NetSuiteClientService<?> clientService = clientFactory.createClient();
        clientService.setEndpointUrl(connectionConfig.getEndpointUrl());
        clientService.setCredentials(connectionConfig.getCredentials());
        clientService.setTokenPassport(connectionConfig.getTokenPassport());
        MetaDataSource metaDataSource = clientService.getMetaDataSource();
        metaDataSource.setCustomizationEnabled(connectionConfig.isCustomizationEnabled());

        clientService.login();

        return clientService;
    }

    public MetaDataSource getMetaDataSource() {
        return clientService.getMetaDataSource();
    }

    /**
     * Holds configuration for connecting to NetSuite.
     */
    @Data
    @EqualsAndHashCode
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectionConfig {

        private String endpointUrl;

        private NetSuiteVersion apiVersion;

        private NetSuiteCredentials credentials;

        private NsTokenPassport tokenPassport;

        private boolean customizationEnabled;
    }
}