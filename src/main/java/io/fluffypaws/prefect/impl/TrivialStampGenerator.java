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

import java.time.Instant;

import io.fluffypaws.prefect.api.Stamp;

/**
 * Trivial implementation for generating stamps for distributed databases. Stamps are used to 1) make transactions
 * uniquely identifiable and globally orderable. This implementation must be replaced with a better implementation
 * that takes into account:
 * - the system time of server can be updated to a "lower" value (due to sysadmin manual action or NTP)
 * - there are multiple servers generating stamps
 */
public class TrivialStampGenerator {

    static Instant lastGeneratedInstant = null;
    static long lastManualNanos = 0;

    static class StampImpl implements Stamp {

        final Instant instant;

        StampImpl(Instant instant) {
            this.instant = instant;
        }

        public boolean isAfter(final Stamp other) {
            StampImpl otherImpl = (StampImpl) other;
            return instant.isAfter(otherImpl.instant);
        }

        public boolean isBefore(final Stamp other) {
            StampImpl otherImpl = (StampImpl) other;
            return instant.isBefore(otherImpl.instant);
        }

        public int compareTo(final Object other) {
            StampImpl otherImpl = (StampImpl) other;
            return instant.compareTo(otherImpl.instant);
        }

        @Override
        public boolean equals(final Object other) {
            if (other instanceof StampImpl) {
                return instant.equals(((StampImpl)other).instant);
            }
            return false;
        }
    }

    public static Stamp generateStamp() {
        Instant now = Instant.now();
        if (now.equals(lastGeneratedInstant)) {
            lastManualNanos++;
            now = now.plusNanos(lastManualNanos);
        } else {
            lastGeneratedInstant = now;
            lastManualNanos = 0;
        }
        return new StampImpl(now);
    }

    public static Stamp calculateSnapshotStamp(Instant instant) {
        return new StampImpl(instant);
    }

}