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
import com.github.nexmark.flink.metric.tps.TpsMetric;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A HTTP client to request TPS metric to JobMaster REST API.
 */
public class FlinkRestClient {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);

	private static int CONNECT_TIMEOUT = 5000;
	private static int SOCKET_TIMEOUT = 60000;
	private static int CONNECTION_REQUEST_TIMEOUT = 10000;
	private static int MAX_IDLE_TIME = 60000;
	private static int MAX_CONN_TOTAL = 60;
	private static int MAX_CONN_PER_ROUTE = 30;

	private final String jmEndpoint;
	private final CloseableHttpClient httpClient;

	public FlinkRestClient(String jmAddress, int jmPort) {
		this.jmEndpoint = jmAddress + ":" + jmPort;

		RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(SOCKET_TIMEOUT)
			.setConnectTimeout(CONNECT_TIMEOUT)
			.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
			.build();
		PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
		httpClientConnectionManager.setValidateAfterInactivity(MAX_IDLE_TIME);
		httpClientConnectionManager.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
		httpClientConnectionManager.setMaxTotal(MAX_CONN_TOTAL);

		this.httpClient = HttpClientBuilder.create()
			.setConnectionManager(httpClientConnectionManager)
			.setDefaultRequestConfig(requestConfig)
			.build();
	}

	public void cancelJob(String jobId) {
		LOG.info("Stopping Job: {}", jobId);
		String url = String.format("http://%s/jobs/%s?mode=cancel", jmEndpoint, jobId);
		patch(url);
	}

	public String getCurrentJobId() {
		String url = String.format("http://%s/jobs", jmEndpoint);
		JsonNode job = executeAsJsonNode(url).get("jobs").get(0);
		checkArgument(
				job.get("status").asText().equals("RUNNING"),
				"The first job is not running.");
		return job.get("id").asText();
	}

	public boolean isJobRunning() {
		String url = String.format("http://%s/jobs", jmEndpoint);
		JsonNode job = executeAsJsonNode(url).get("jobs").get(0);
		return job.get("status").asText().equals("RUNNING");
	}

	public String getSourceVertexId(String jobId) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		JsonNode sourceVertex = executeAsJsonNode(url).get("vertices").get(0);
		checkArgument(
				sourceVertex.get("name").asText().startsWith("Source:"),
				"The first vertex is not a source.");
		return sourceVertex.get("id").asText();
	}

	public String getTpsMetricName(String jobId, String vertexId) {
		String url = String.format("http://%s/jobs/%s/vertices/%s/subtasks/metrics", jmEndpoint, jobId, vertexId);
		ArrayNode metrics = (ArrayNode) executeAsJsonNode(url);
		for (JsonNode metric : metrics) {
			String metricName = metric.get("id").asText();
			if (metricName.startsWith("Source_") && metricName.endsWith(".numRecordsOutPerSecond")) {
				return metricName;
			}
		}
		throw new RuntimeException("Can't find TPS metric name from the response");
	}

	public List<String> getSinkMeanLatencyMetricNames(String jobId, String vertexId) {
		String url = String.format("http://%s/jobs/%s/metrics", jmEndpoint, jobId);
		ArrayNode metrics = (ArrayNode) executeAsJsonNode(url);
		List<String> metricNames = new ArrayList<>();
		for (JsonNode metric : metrics) {
			String metricName = metric.get("id").asText();
			if (metricName.contains(vertexId) && metricName.endsWith("latency_mean")) {
				metricNames.add(metricName);
			}
		}
		return metricNames;
	}

	public synchronized TpsMetric getTpsMetric(String jobId, String vertexId, String tpsMetricName) {
		String url = String.format(
			"http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s",
			jmEndpoint,
			jobId,
			vertexId,
			tpsMetricName);
		String response = executeAsString(url);
		return TpsMetric.fromJson(response);
	}

	public synchronized SinkMeanLatencyMetric getSinkMeanLatencyMetric(String jobId, String latencyMetricName) {
		String url = String.format(
				"http://%s/jobs/%s/metrics?get=%s",
				jmEndpoint,
				jobId,
				latencyMetricName);
		String response = executeAsString(url);
		return SinkMeanLatencyMetric.fromJson(response);
	}

	private void patch(String url) {
		HttpPatch httpPatch = new HttpPatch();
		httpPatch.setURI(URI.create(url));
		HttpResponse response;
		try {
			httpPatch.setHeader("Connection", "close");
			response = httpClient.execute(httpPatch);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_ACCEPTED) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
		} catch (Exception e) {
			httpPatch.abort();
			throw new RuntimeException(e);
		}
	}

	private JsonNode executeAsJsonNode(String url) {
		String response = executeAsString(url);
		try {
			return NexmarkUtils.MAPPER.readTree(response);
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	private String executeAsString(String url) {
		HttpGet httpGet = new HttpGet();
		httpGet.setURI(URI.create(url));
		try {
			HttpEntity entity = execute(httpGet).getEntity();
			if (entity != null) {
				return EntityUtils.toString(entity, Consts.UTF_8);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to request URL " + url, e);
		}
		throw new RuntimeException(String.format("Response of URL %s is null.", url));
	}

	private HttpResponse execute(HttpRequestBase httpRequestBase) throws Exception {
		HttpResponse response;
		try {
			httpRequestBase.setHeader("Connection", "close");
			response = httpClient.execute(httpRequestBase);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_OK) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
			return response;
		} catch (Exception e) {
			httpRequestBase.abort();
			throw e;
		}
	}

	public synchronized void close() {
		try {
			if (httpClient != null) {
				httpClient.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
