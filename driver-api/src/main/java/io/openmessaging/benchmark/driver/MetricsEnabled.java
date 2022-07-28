/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver;

import java.util.function.BiConsumer;

public interface MetricsEnabled {

    public enum Combiner {
        SUM,
        MAX,
        /**
         * WARNING: when using multiple producers/consumers per worker or multiple workers
         * this will take an average of the averages - which may not be representative when
         * the value is not uniform.
         *
         * Typically the raw state will be a running average, not reset per period
         */
        AVERAGE,
    }

    public static class Metric {
        Combiner combiner;
        double value;
        String units;
        
        public Metric() {
            
        }

        public Metric(Combiner combiner, double value, String units) {
            this.combiner = combiner;
            this.value = value;
            this.units = units;
        }

        public Combiner getCombiner() {
            return combiner;
        }

        public double getValue() {
            return value;
        }

        public String getUnits() {
            return units;
        }

    }

    default void supplyMetrics(BiConsumer<String, Metric> consumer) {

    }
}
