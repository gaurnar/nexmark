/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.metric;

import com.github.nexmark.flink.metric.latency.SinkMeanLatencyMetric;
import org.apache.flink.api.common.time.Deadline;

import com.github.nexmark.flink.metric.cpu.CpuMetricReceiver;
import com.github.nexmark.flink.metric.tps.TpsMetric;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatDoubleValue;

/**
 * A reporter to aggregate metrics and report summary results.
 */
public class MetricReporter {

	private static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

	private final Duration monitorDelay;
	private final Duration monitorInterval;
	private final Duration monitorDuration;
	private final FlinkRestClient flinkRestClient;
	private final CpuMetricReceiver cpuMetricReceiver;
	private final List<BenchmarkMetric> metrics;
	private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
	private volatile Throwable error;

	public MetricReporter(FlinkRestClient flinkRestClient, CpuMetricReceiver cpuMetricReceiver, Duration monitorDelay, Duration monitorInterval, Duration monitorDuration) {
		this.monitorDelay = monitorDelay;
		this.monitorInterval = monitorInterval;
		this.monitorDuration = monitorDuration;
		this.flinkRestClient = flinkRestClient;
		this.cpuMetricReceiver = cpuMetricReceiver;
		this.metrics = new ArrayList<>();
	}

	private void submitMonitorThread(long eventsNum) {
		JobInformation jobInfo;

		while ((jobInfo = getJobInformation()) == null) {
			// wait for the job startup
			try {
				//noinspection BusyWait
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		this.service.scheduleWithFixedDelay(
			new MetricCollector(jobInfo.jobId, jobInfo.vertexId, jobInfo.tpsMetricName,
					jobInfo.sinkMeanLatencyMetricNames, eventsNum),
			0L,
			monitorInterval.toMillis(),
			TimeUnit.MILLISECONDS
		);
	}

	private JobInformation getJobInformation() {
		try {
			String jobId = flinkRestClient.getCurrentJobId();
			String vertexId = flinkRestClient.getSourceVertexId(jobId);
			String tpsMetricName = flinkRestClient.getTpsMetricName(jobId, vertexId);
			List<String> sinkMeanLatencyMetricNames = flinkRestClient.getSinkMeanLatencyMetricNames(jobId, vertexId);
			return new JobInformation(jobId, vertexId, tpsMetricName, sinkMeanLatencyMetricNames);
		} catch (Exception e) {
			LOG.warn("Job metric is not ready yet.", e);
			return null;
		}
	}

	private void waitFor(Duration duration) {
		Deadline deadline = Deadline.fromNow(duration);
		while (deadline.hasTimeLeft()) {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (error != null) {
				throw new RuntimeException(error);
			}
		}
	}

	private boolean isJobRunning() {
		return flinkRestClient.isJobRunning();
	}

	private void waitForOrJobFinish() {
		while (isJobRunning()) {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (error != null) {
				throw new RuntimeException(error);
			}
		}
	}

	public JobBenchmarkMetric reportMetric(long eventsNum) {
		System.out.printf("Monitor metrics after %s seconds.%n", monitorDelay.getSeconds());
		long startTime = System.currentTimeMillis();
		waitFor(monitorDelay);
		if (eventsNum == 0) {
			System.out.printf("Start to monitor metrics for %s seconds.%n", monitorDuration.getSeconds());
		} else {
			System.out.println("Start to monitor metrics until job is finished.");
		}
		submitMonitorThread(eventsNum);
		if (eventsNum == 0) {
			waitFor(monitorDuration);
		} else {
			waitForOrJobFinish();
		}

		long endTime = System.currentTimeMillis();

		// cleanup the resource
		this.close();

		if (metrics.isEmpty()) {
			throw new RuntimeException("The metric reporter doesn't collect any metrics.");
		}
		double sumTps = 0.0;
		double sumLatency = 0.0;
		double sumCpu = 0.0;
		int latencyMetricsCount = 0;

		for (BenchmarkMetric metric : metrics) {
			sumTps += metric.getTps();
			if (metric.getLatency() != null) {
				sumLatency += metric.getLatency();
				latencyMetricsCount++;
			}
			sumCpu += metric.getCpu();
		}

		double avgTps = sumTps / metrics.size();
		Double avgLatency = null;
		if (latencyMetricsCount != 0) {
			avgLatency = sumLatency / latencyMetricsCount;
		}
		double avgCpu = sumCpu / metrics.size();
		JobBenchmarkMetric metric = new JobBenchmarkMetric(
				avgTps, avgLatency, avgCpu, eventsNum, endTime - startTime);

		String message;
		if (eventsNum == 0) {
			if (avgLatency != null) {
				message = String.format("Summary Average: Throughput=%s, Latency=%s, Cores=%s",
						metric.getPrettyTps(),
						metric.getPrettyLatency(),
						metric.getPrettyCpu());
			} else {
				message = String.format("Summary Average: Throughput=%s, Cores=%s",
						metric.getPrettyTps(),
						metric.getPrettyCpu());
			}
		} else {
			message = String.format("Summary Average: EventsNum=%s, Cores=%s, Time=%s s",
					NUMBER_FORMAT.format(eventsNum),
					metric.getPrettyCpu(),
					formatDoubleValue(metric.getTimeSeconds()));
		}

		System.out.println(message);
		LOG.info(message);
		return metric;
	}

	public void close() {
		service.shutdownNow();
	}

	private class MetricCollector implements Runnable {
		private final String jobId;
		private final String sourceVertexId;
		private final String tpsMetricName;
		private final List<String> sinkMeanLatencyMetricNames;
		private final long eventsNum;

		private MetricCollector(String jobId, String sourceVertexId, String tpsMetricName,
								List<String> sinkMeanLatencyMetricNames, long eventsNum) {
			this.jobId = jobId;
			this.sourceVertexId = sourceVertexId;
			this.tpsMetricName = tpsMetricName;
			this.sinkMeanLatencyMetricNames = sinkMeanLatencyMetricNames;
			this.eventsNum = eventsNum;
		}

		@Override
		public void run() {
			try {
				TpsMetric tps = flinkRestClient.getTpsMetric(jobId, sourceVertexId, tpsMetricName);

				Double sinkMeanLatency = null;
				if (!sinkMeanLatencyMetricNames.isEmpty()) {
					sinkMeanLatency = sinkMeanLatencyMetricNames.stream()
							.map(metricName -> flinkRestClient.getSinkMeanLatencyMetric(jobId, metricName))
							.collect(Collectors.averagingDouble(SinkMeanLatencyMetric::getValue));
				}

				double cpu = cpuMetricReceiver.getTotalCpu();
				int tms = cpuMetricReceiver.getNumberOfTM();

				BenchmarkMetric metric = new BenchmarkMetric(tps.getSum(), cpu, sinkMeanLatency);
				// it's thread-safe to update metrics
				metrics.add(metric);
				// logging
				String message;
				if (eventsNum == 0) {
					if (sinkMeanLatency != null) {
						message = String.format("Current Throughput=%s, Latency=%s, Cores=%s (%s TMs)",
								metric.getPrettyTps(), metric.getPrettyLatency(), metric.getPrettyCpu(), tms);
					} else {
						message = String.format("Current Throughput=%s, Cores=%s (%s TMs)",
								metric.getPrettyTps(), metric.getPrettyCpu(), tms);
					}
				} else {
					message = String.format("Current Cores=%s (%s TMs)", metric.getPrettyCpu(), tms);
				}
				System.out.println(message);
				LOG.info(message);
			} catch (Exception e) {
				error = e;
			}
		}
	}

	private static class JobInformation {
		final String jobId;
		final String vertexId;
		final String tpsMetricName;
		final List<String> sinkMeanLatencyMetricNames;

		JobInformation(String jobId, String vertexId, String tpsMetricName, List<String> sinkMeanLatencyMetricNames) {
			this.jobId = jobId;
			this.vertexId = vertexId;
			this.tpsMetricName = tpsMetricName;
			this.sinkMeanLatencyMetricNames = sinkMeanLatencyMetricNames;
		}
	}
}
