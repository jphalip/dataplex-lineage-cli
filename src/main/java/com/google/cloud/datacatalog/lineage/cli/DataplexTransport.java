/*
* Copyright 2024 Google Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.google.cloud.datacatalog.lineage.cli;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datacatalog.lineage.v1.LineageSettings;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventRequest;
import com.google.cloud.datacatalog.lineage.v1.ProcessOpenLineageRunEventResponse;
import com.google.cloud.datalineage.producerclient.helpers.OpenLineageHelper;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageClient;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageProducerClient;
import com.google.cloud.datalineage.producerclient.v1.AsyncLineageProducerClientSettings;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClient;
import com.google.cloud.datalineage.producerclient.v1.SyncLineageProducerClientSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataplexTransport implements AutoCloseable {

  @Getter private final ProducerClientWrapper producerClientWrapper;

  protected DataplexTransport(@NonNull ProducerClientWrapper client) throws IOException {
    this.producerClientWrapper = client;
  }

  @Override
  public void close() {
    producerClientWrapper.close();
  }

  static class ProducerClientWrapper implements Closeable {
    private final SyncLineageClient syncLineageClient;
    private final AsyncLineageClient asyncLineageClient;
    private final String parent;

    protected ProducerClientWrapper(DataplexConfig config) throws IOException {
      LineageSettings settings;
      if (DataplexConfig.Mode.sync == config.getMode()) {
        settings = createSyncSettings(config);
        syncLineageClient =
            SyncLineageProducerClient.create((SyncLineageProducerClientSettings) settings);
        asyncLineageClient = null;
      } else {
        syncLineageClient = null;
        settings = createAsyncSettings(config);
        asyncLineageClient =
            AsyncLineageProducerClient.create((AsyncLineageProducerClientSettings) settings);
      }
      this.parent = getParent(config, settings);
    }

    protected ProducerClientWrapper(DataplexConfig config, SyncLineageClient client)
        throws IOException {
      this.syncLineageClient = client;
      this.parent = getParent(config, createAsyncSettings(config));
      this.asyncLineageClient = null;
    }

    protected ProducerClientWrapper(DataplexConfig config, AsyncLineageClient client)
        throws IOException {
      this.asyncLineageClient = client;
      this.parent = getParent(config, createSyncSettings(config));
      this.syncLineageClient = null;
    }

    public void emitEvent(String eventJson) throws InvalidProtocolBufferException {
      Struct openLineageStruct = OpenLineageHelper.jsonToStruct(eventJson);
      ProcessOpenLineageRunEventRequest request =
          ProcessOpenLineageRunEventRequest.newBuilder()
              .setParent(parent)
              .setOpenLineage(openLineageStruct)
              .build();
      if (syncLineageClient != null) {
        ProcessOpenLineageRunEventResponse response = syncLineageClient.processOpenLineageRunEvent(request);
        System.out.println("Event name: " + response.getLineageEvents(0));
      } else {
        handleRequestAsync(request);
      }
    }

    private void handleRequestAsync(ProcessOpenLineageRunEventRequest request) {
      ApiFuture<ProcessOpenLineageRunEventResponse> future =
          asyncLineageClient.processOpenLineageRunEvent(request);
      ApiFutureCallback<ProcessOpenLineageRunEventResponse> callback =
          new ApiFutureCallback<ProcessOpenLineageRunEventResponse>() {
            @Override
            public void onFailure(Throwable t) {
              log.error("Failed to collect a lineage event: {}", request.getOpenLineage(), t);
            }

            @Override
            public void onSuccess(ProcessOpenLineageRunEventResponse result) {
              System.out.println("Event name: " + result.getLineageEvents(0));
              log.debug("Event sent successfully: {}", request.getOpenLineage());
            }
          };
      ApiFutures.addCallback(future, callback, MoreExecutors.directExecutor());
    }

    private String getParent(DataplexConfig config, LineageSettings settings) throws IOException {
      return String.format(
          "projects/%s/locations/%s",
          getProjectId(config, settings),
          config.getLocation() != null ? config.getLocation() : "us");
    }

    private static SyncLineageProducerClientSettings createSyncSettings(DataplexConfig config)
        throws IOException {
      SyncLineageProducerClientSettings.Builder builder =
          SyncLineageProducerClientSettings.newBuilder();
      return createSettings(config, builder).build();
    }

    private static AsyncLineageProducerClientSettings createAsyncSettings(DataplexConfig config)
        throws IOException {
      AsyncLineageProducerClientSettings.Builder builder =
          AsyncLineageProducerClientSettings.newBuilder();
      return createSettings(config, builder).build();
    }

    private static <T extends LineageSettings.Builder> T createSettings(
        DataplexConfig config, T builder) throws IOException {
      if (config.getEndpoint() != null) {
        builder.setEndpoint(config.getEndpoint());
      }
      if (config.getProjectId() != null) {
        builder.setQuotaProjectId(config.getProjectId());
      }
      if (config.getCredentialsFile() != null) {
        File file = new File(config.getCredentialsFile());
        try (InputStream credentialsStream = Files.newInputStream(file.toPath())) {
          GoogleCredentials googleCredentials = GoogleCredentials.fromStream(credentialsStream);
          builder.setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials));
        }
      }
      return builder;
    }

    private static String getProjectId(DataplexConfig config, LineageSettings settings)
        throws IOException {
      if (config.getProjectId() != null) {
        return config.getProjectId();
      }
      Credentials credentials = settings.getCredentialsProvider().getCredentials();
      if (credentials instanceof ServiceAccountCredentials) {
        ServiceAccountCredentials serviceAccountCredentials =
            (ServiceAccountCredentials) credentials;
        return serviceAccountCredentials.getProjectId();
      }
      if (credentials instanceof GoogleCredentials) {
        GoogleCredentials googleCredentials = (GoogleCredentials) credentials;
        return googleCredentials.getQuotaProjectId();
      }
      return settings.getQuotaProjectId();
    }

    @Override
    public void close() {
      try {
        if (syncLineageClient != null) {
          syncLineageClient.close();
        }
        if (asyncLineageClient != null) {
          asyncLineageClient.close();
        }
      } catch (Exception e) {
        throw new RuntimeException("Exception while closing the resource", e);
      }
    }
  }
}
