package org.talend.components.netsuite.service;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;

@Service
public class UIActionService {

    @Service
    private NetsuiteService service;

    @Service
    private LocalConfiguration configuration;

    @HealthCheck("connection.healthcheck")
    public HealthCheckStatus validateConnection(@Option final NetsuiteDataStore dataStore, final Messages i18n,
            LocalConfiguration configuration) {
        try {
            this.service.connect(dataStore);
        } catch (Exception e) {
            return new HealthCheckStatus(Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(Status.OK, i18n.healthCheckOk());
    }

    @DiscoverSchema("guessInputSchema")
    public Schema guessInputSchema(@Option final NetsuiteInputDataSet dataSet) {
        return new Schema(service.getSchema(dataSet.getCommonDataSet()));
    }

    @DiscoverSchema("guessOutputSchema")
    public Schema guessOutputSchema(@Option final NetsuiteOutputDataSet dataSet) {
        return new Schema(service.getSchema(dataSet.getCommonDataSet()));
    }

    @Suggestions("loadRecordTypes")
    public SuggestionValues loadRecordTypes(@Option final NetsuiteDataStore dataStore) {
        try {
            List<SuggestionValues.Item> items = service.getRecordTypes(dataStore);
            return new SuggestionValues(true, items);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Suggestions("loadFields")
    public SuggestionValues loadFields(@Option final NetSuiteCommonDataSet dataSet) {
        return StringUtils.isEmpty(dataSet.getRecordType()) ? new SuggestionValues(false, Collections.emptyList())
                : new SuggestionValues(true, service.getSearchTypes(dataSet));
    }

    @Suggestions("loadOperators")
    public SuggestionValues loadOperators(@Option("dataStore") final NetsuiteDataStore dataStore) {
        return new SuggestionValues(true, service.getSearchFieldOperators(dataStore));
    }
}