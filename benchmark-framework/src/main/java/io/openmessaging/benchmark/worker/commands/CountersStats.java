/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.worker.commands;

import java.util.LinkedHashMap;
import java.util.Map;

import io.openmessaging.benchmark.driver.MetricsEnabled.Metric;

public class CountersStats {
    public long messagesSent;
    public long messagesReceived;
    public double elapsedMillis;
    public long publishErrors;
    public long consumerErrors;
    public int producers;
    public int consumers;

    public Map<String, Metric> additionalMetrics = new LinkedHashMap<>();

}
