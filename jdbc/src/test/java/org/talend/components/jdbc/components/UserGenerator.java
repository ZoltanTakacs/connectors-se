/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.components;

import static java.util.Optional.ofNullable;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;

@Version
@Icon(Icon.IconType.SAMPLE)
@Emitter(name = "UserGenerator", family = "jdbcTest")
public class UserGenerator implements Serializable {

    private final Config config;

    private RecordBuilderFactory recordBuilderFactory;

    private Queue<Record> data = new LinkedList<>();

    public UserGenerator(@Option("config") final Config config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @PostConstruct
    public void init() {
        data.addAll(IntStream.range(1, config.rowCount + 1)
                .mapToObj(i -> recordBuilderFactory.newRecordBuilder().withInt("id", i)
                        .withString("name", ofNullable(config.namePrefix).orElse("user") + i).build())
                .collect(Collectors.toList()));
    }

    @Producer
    public Record next() {
        return data.poll();
    }

    @PreDestroy
    public void close() {
        data = new LinkedList<>();
    }

    @Data
    public static class Config {

        @Option
        private int rowCount;

        private String namePrefix;
    }

}
