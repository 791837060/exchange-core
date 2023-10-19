/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.tests.perf;

import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static exchange.core2.tests.util.LatencyTestsModule.latencyTestImpl;

@Slf4j
public final class PerfLatencyJournaling {

    /*
     * -------------- Disk Journaling tests -----------------
     * Recommended tuned profile: latency-performance
     * 推荐的优化配置文件：延迟性能
     *
     */

    @Test
    public void testLatencyMarginJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                6);
    }

    @Test
    public void testLatencyExchangeJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,// snapshots and journaling
                6);
    }

    @Test
    public void testLatencyMultiSymbolMediumJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,// snapshots and journaling
                3); // 预热周期
    }

    @Test
    public void testLatencyMultiSymbolLargeJournaling() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,// snapshots and journaling
                3);
    }

    @Test
    public void testLatencyMultiSymbolHugeJournaling() { // 测试延迟多符号巨大日志
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.hugeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,// snapshots and journaling
                2);
    }
}