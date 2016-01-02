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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fluffypaws.prefect.api.Key;
import io.fluffypaws.prefect.api.KeyValueStore;
import io.fluffypaws.prefect.api.KeyValueStoreStatistics;
import io.fluffypaws.prefect.api.Stamp;
import io.fluffypaws.prefect.api.StoreException;
import io.fluffypaws.prefect.api.StoreFactory;
import io.fluffypaws.prefect.api.Value;

public class FileSystemKeyValueStore implements KeyValueStore {

    private static Logger log = LoggerFactory.getLogger(FileSystemKeyValueStore.class);

    private long firstKey = 0;
    private long highestKey = firstKey;
    private String storeDirectoryName;
    private KeyValueStoreStatistics stats = new KeyValueStoreStatistics();

    public FileSystemKeyValueStore() {
    }

    public boolean initialize(final Properties properties) {
        if (!properties.containsKey(StoreFactory.STORE_DIRECTORY_KEY)) {
            log.error("Cannot initialize FileSystemKeyValueStore, cannot find property: " + StoreFactory.STORE_DIRECTORY_KEY);
            return false;
        }

        storeDirectoryName = properties.getProperty(StoreFactory.STORE_DIRECTORY_KEY);

        if (!Files.isDirectory(Paths.get(storeDirectoryName))) {
            log.error("Cannot initialize FileSystemKeyValueStore, cannot find directory: " + storeDirectoryName);
            return false;
        }

        log.info("FileSystemKeyValueStore starting in " + storeDirectoryName);

        try {
            Value value = read(getFirstKey());
            if (value != null) {
                log.error("Cannot initialize FileSystemKeyValueStore, cannot start in a previously initialized directory (yet).");
                return false;
            }
        } catch (StoreException e) {
            log.error("Cannot initialize FileSystemKeyValueStore, error reading initial object", e);
            return false;
        }

        try {
            write(getFirstKey(), new Value(), false);
        } catch (StoreException e) {
            log.error("Cannot initialize FileSystemKeyValueStore, cannot write initial object", e);
            return false;
        }

        return true;
    }

    private Key longToKey(long l) {
        return new Key(String.valueOf(l).getBytes(StandardCharsets.UTF_8));
    }

    public Key getFirstKey() {
        return longToKey(firstKey);
    }

    private String keyToFileName(Key key) {
        return storeDirectoryName + "/" + new String(key.getData(), StandardCharsets.UTF_8);
    }

    public Key add(final Value value) throws StoreException {
        highestKey++;
        Key key = longToKey(highestKey);

        write(key, value, false);

        return key;
    }

    public Value read(final Key key) throws StoreException {
        String fileName = keyToFileName(key);

        if (!Files.isRegularFile(Paths.get(fileName))) {
            return null;
        }

        try {
            return new Value(Files.readAllBytes(Paths.get(fileName)));
        } catch (IOException e) {
            log.error("Error reading " + fileName, e);
            throw new StoreException(e);
        }
    }

    private void write(final Key key, final Value value, boolean expected) throws StoreException {
        String fileName = keyToFileName(key);

        boolean exists = Files.isRegularFile(Paths.get(fileName));
        if (expected && !exists) {
            log.error("SEVERE - write called for a key that does not exist yet");
        }
        if (!expected && exists) {
            log.error("SEVERE - unexpected value found while writing");
        }

        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            fos.write(value.getData());
            stats.totalWrites++;
            stats.totalBytesWritten += value.getData().length;
        } catch (IOException e) {
            log.error("Error writing " + fileName, e);
            throw new StoreException(e);
        }
    }

    public void write(final Key key, final Value value) throws StoreException {
        write(key, value, true);
    }

    public void delete(final Key key) throws StoreException, IllegalArgumentException {
        if (Arrays.equals(key.getData(), getFirstKey().getData())) {
            throw new IllegalArgumentException("Cannot delete first key");
        }

        String fileName = keyToFileName(key);

        try {
            Files.delete(Paths.get(fileName));
        } catch (FileNotFoundException e) {
            // ok
        } catch (IOException e) {
            log.error("Error deleting " + fileName, e);
            throw new StoreException(e);
        }
    }

    public Stamp generateStamp() {
        return TrivialStampGenerator.generateStamp();
    }

    public Stamp calculateSnapshotStamp(final Instant instant) {
        return TrivialStampGenerator.calculateSnapshotStamp(instant);
    }

    public KeyValueStoreStatistics getStatistics() {
        return stats;
    }

}