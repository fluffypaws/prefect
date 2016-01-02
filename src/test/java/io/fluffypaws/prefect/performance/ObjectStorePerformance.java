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

package io.fluffypaws.prefect.performance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import io.fluffypaws.prefect.api.KeyValueStoreStatistics;
import io.fluffypaws.prefect.api.Object;
import io.fluffypaws.prefect.api.ObjectStore;
import io.fluffypaws.prefect.api.StoreFactory;

import static org.junit.Assert.fail;

public class ObjectStorePerformance {

    public static void main(String[] args) {
        try {
            Path path = Files.createTempDirectory("ObjectStorePerformance");

            Properties properties = new Properties();
            properties.setProperty(StoreFactory.STORE_DIRECTORY_KEY, path.toAbsolutePath().toString());
            ObjectStore store = StoreFactory.createObjectStore(properties);
            if (store == null) {
                fail("Error initializing store");
            }

            Object root = store.getRoot();

            Instant start = Instant.now();

            int num = 100;
            for (int i = 0; i < num; i++) {
                Object childObject = root.addChild(String.valueOf(i), null);
                for (int j = 0; j < num; j++) {
                    childObject.addChild(String.valueOf(j), null);
                }
            }

            Instant end = Instant.now();
            long gap = ChronoUnit.MILLIS.between(start, end);
            float avg = (float)gap/(float)(num*num);
            System.out.println("Time elapsed: " + gap + " avg(ms): " + avg + " trx/sec: " + 1000.0/avg);
            KeyValueStoreStatistics stats = store.getKeyValueStore().getStatistics();
            System.out.println("Stats:");
            System.out.println("totalWrites:       " + stats.totalWrites);
            System.out.println("totalBytesWritten: " + stats.totalBytesWritten);
            System.out.println("avg bytes/write  : " + stats.totalBytesWritten / stats.totalWrites);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}