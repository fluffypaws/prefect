/*
 *  Copyright 2016 FluffyPaws Inc. (http://www.fluffypaws.io)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.fluffypaws.prefect.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import io.fluffypaws.prefect.api.Key;
import io.fluffypaws.prefect.api.KeyValueStore;
import io.fluffypaws.prefect.api.KeyValueStoreStatistics;
import io.fluffypaws.prefect.api.Stamp;
import io.fluffypaws.prefect.api.StoreException;
import io.fluffypaws.prefect.api.StoreFactory;
import io.fluffypaws.prefect.api.Value;

public class CassandraKeyValueStore implements KeyValueStore {

    private static Logger log = LoggerFactory.getLogger(CassandraKeyValueStore.class);
    private static CassandraDaemon cassandraDaemon = null;

    private final String CASSANDRA_CONFIG_FILENAME = "cassandra.yaml";

    public CassandraKeyValueStore() {
    }

    /**
     * Generates a configuration file for Cassandra with a certain directory where all data files are or will be stored.
     * @param fileName the full path to the configuration file that must be generated
     * @param directoryName the full path to the directory where all data files are or will be stored
     * @return
     */
    private boolean generateCassandraConfig(String fileName, String directoryName) {
        try {
            Configuration cfg = new Configuration();
            cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/"));
            Template template = cfg.getTemplate("cassandra.yaml.ftl");

            Map<String, Object> data = new HashMap<>();
            data.put("data_file_directory", directoryName + "/data_file");
            data.put("commitlog_directory", directoryName + "/commitlog");
            data.put("saved_caches_directory", directoryName + "/saved_caches");

            Writer file = new FileWriter(new File(fileName));
            template.process(data, file);
            file.flush();
            file.close();

            return true;
        } catch (IOException|TemplateException e) {
            log.error("Cannot generate cassandra config file from template", e);
            return false;
        }
    }

    public boolean initialize(Properties properties) {
        if (!properties.containsKey(StoreFactory.STORE_DIRECTORY_KEY)) {
            log.error("Cannot initialize CassandraKeyValueStore, cannot find property: " + StoreFactory.STORE_DIRECTORY_KEY);
            return false;
        }

        String storeDirectoryName = properties.getProperty(StoreFactory.STORE_DIRECTORY_KEY);

        if (!Files.isDirectory(Paths.get(storeDirectoryName))) {
            log.error("Cannot initialize CassandraKeyValueStore, cannot find directory: " + storeDirectoryName);
            return false;
        }

        String cassandraConfigFileName = storeDirectoryName + "/" + CASSANDRA_CONFIG_FILENAME;

        if (!Files.isRegularFile(Paths.get(cassandraConfigFileName))) {
            log.info("Cassandra config file not found, generating " + cassandraConfigFileName);

            if (!generateCassandraConfig(cassandraConfigFileName, storeDirectoryName)) {
                return false;
            }
        }

        // Used the following for inspiration: https://github.com/jsevellec/cassandra-unit/blob/21dc8a443a78021e0f1afebd4712cfed6ed3fce0/cassandra-unit/src/main/java/org/cassandraunit/utils/EmbeddedCassandraServerHelper.java
        System.setProperty("cassandra.config", "file:///" + cassandraConfigFileName);

        cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.activate();

        return cassandraDaemon.setupCompleted();
    }

    public void shutdown() {
        cassandraDaemon.deactivate();
        cassandraDaemon = null;
    }

    public Key getFirstKey() {
        return null;
    }

    public Key add(final Value value) throws StoreException {
        return null;
    }

    public Value read(final Key key) throws StoreException {
        return null;
    }

    public void write(final Key key, final Value value) throws StoreException {
    }

    public void delete(final Key key) throws StoreException, IllegalArgumentException {
    }

    public Stamp generateStamp() {
        return TrivialStampGenerator.generateStamp();
    }

    public Stamp calculateSnapshotStamp(final Instant instant) {
        return TrivialStampGenerator.calculateSnapshotStamp(instant);
    }

    public KeyValueStoreStatistics getStatistics() {
        return null;
    }

}