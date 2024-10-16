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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.datacatalog.lineage.v1.LocationName;
import com.google.cloud.datacatalog.lineage.v1.Process;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import picocli.CommandLine;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DataplexCLIIntegrationTest {

  private static final String PROJECT_ID = System.getenv("PROJECT");
  private static final String LOCATION = "us-central1";
  protected String olJobNamespace;
  protected String olJobName;
  protected Process dataplexProcess;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  public void setupEach(TestInfo testInfo) {
    String className = testInfo.getTestClass().map(Class::getSimpleName).orElse("UnknownClass");
    String methodName = testInfo.getTestMethod().map(Method::getName).orElse("UnknownMethod");
    olJobName = String.format("%s.%s", className, methodName);
    olJobNamespace =
        String.format(
            "test_namespace_%d_%d",
            Long.MAX_VALUE - System.currentTimeMillis(), Long.MAX_VALUE - System.nanoTime());
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void tearDownEach() throws IOException {
    Process process = getDataplexProcess();
    if (process != null) {
      DataplexUtils.deleteProcess(process);
    }
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  public Process getDataplexProcess() throws IOException {
    if (dataplexProcess == null) {
      return DataplexUtils.getProcessWithDisplayName(
          getDataplexLocationName(), String.format("%s:%s", olJobNamespace, olJobName));
    }
    return dataplexProcess;
  }

  public static LocationName getDataplexLocationName() {
    return LocationName.of(PROJECT_ID, LOCATION);
  }

  public JsonElement extractLastJson() {
    String content = outContent.toString();

    int startIndex = content.indexOf("[");
    if (startIndex == -1) {
      return null; // No JSON array found
    }

    // Find the matching closing bracket
    int count = 1;
    int index = startIndex + 1;
    while (count > 0 && index < content.length()) {
      char c = content.charAt(index);
      if (c == '[') {
        count++;
      } else if (c == ']') {
        count--;
      }
      index++;
    }

    if (count != 0) {
      return null; // Unmatched brackets, invalid JSON array
    }

    String jsonStr = content.substring(startIndex, index);

    try {
      return JsonParser.parseString(jsonStr);
    } catch (JsonSyntaxException e) {
      return null; // Invalid JSON
    }
  }

  int emitOpenLineageEvent() {
    String eventJson =
        String.format(
            "{\n"
                + "  \"eventTime\": \"2024-10-09T17:58:53.568Z\",\n"
                + "  \"producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "  \"schemaURL\": \"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent\",\n"
                + "  \"eventType\": \"COMPLETE\",\n"
                + "  \"run\": {\n"
                + "    \"runId\": \"%s\",\n"
                + "    \"facets\": {\n"
                + "      \"processing_engine\": {\n"
                + "        \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "        \"_schemaURL\": \"https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet\",\n"
                + "        \"version\": \"3.1.3\",\n"
                + "        \"name\": \"hive\",\n"
                + "        \"openlineageAdapterVersion\": \"1.0.0-SNAPSHOT\"\n"
                + "      },\n"
                + "      \"hive_properties\": {\n"
                + "        \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "        \"_schemaURL\": \"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet\",\n"
                + "        \"properties\": {\n"
                + "          \"hive.query.id\": \"hive_20241009175838_80d7821e-2604-48df-b2de-a00435ecdf32\",\n"
                + "          \"hive.execution.engine\": \"tez\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"job\": {\n"
                + "    \"namespace\": \"%s\",\n"
                + "    \"name\": \"%s\"\n"
                + "  },\n"
                + "  \"inputs\": [\n"
                + "    {\n"
                + "      \"namespace\": \"gs://example-bucket\",\n"
                + "      \"name\": \"warehouse/transactions\",\n"
                + "      \"facets\": {\n"
                + "        \"schema\": {\n"
                + "          \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "          \"_schemaURL\": \"https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet\",\n"
                + "          \"fields\": [\n"
                + "            {\n"
                + "              \"name\": \"submissiondate\",\n"
                + "              \"type\": \"date\"\n"
                + "            },\n"
                + "            {\n"
                + "              \"name\": \"transactionamount\",\n"
                + "              \"type\": \"double\"\n"
                + "            },\n"
                + "            {\n"
                + "              \"name\": \"transactiontype\",\n"
                + "              \"type\": \"string\"\n"
                + "            }\n"
                + "          ]\n"
                + "        },\n"
                + "        \"symlinks\": {\n"
                + "          \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "          \"_schemaURL\": \"https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet\",\n"
                + "          \"identifiers\": [\n"
                + "            {\n"
                + "              \"namespace\": \"hive\",\n"
                + "              \"name\": \"default.transactions\",\n"
                + "              \"type\": \"TABLE\"\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ],\n"
                + "  \"outputs\": [\n"
                + "    {\n"
                + "      \"namespace\": \"hdfs://hive-lineage-cluster-m\",\n"
                + "      \"name\": \"/user/hive/warehouse/monthly_transaction_summary\",\n"
                + "      \"facets\": {\n"
                + "        \"schema\": {\n"
                + "          \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "          \"_schemaURL\": \"https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet\",\n"
                + "          \"fields\": [\n"
                + "            {\n"
                + "              \"name\": \"month\",\n"
                + "              \"type\": \"string\"\n"
                + "            },\n"
                + "            {\n"
                + "              \"name\": \"transactiontype\",\n"
                + "              \"type\": \"string\"\n"
                + "            },\n"
                + "            {\n"
                + "              \"name\": \"totalamount\",\n"
                + "              \"type\": \"double\"\n"
                + "            },\n"
                + "            {\n"
                + "              \"name\": \"transactioncount\",\n"
                + "              \"type\": \"bigint\"\n"
                + "            }\n"
                + "          ]\n"
                + "        },\n"
                + "        \"columnLineage\": {\n"
                + "          \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "          \"_schemaURL\": \"https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet\",\n"
                + "          \"fields\": {\n"
                + "            \"month\": {\n"
                + "              \"inputFields\": [\n"
                + "                {\n"
                + "                  \"namespace\": \"gs://example-bucket\",\n"
                + "                  \"name\": \"warehouse/transactions\",\n"
                + "                  \"field\": \"submissiondate\",\n"
                + "                  \"transformations\": [\n"
                + "                    {\n"
                + "                      \"type\": \"DIRECT\",\n"
                + "                      \"subtype\": \"TRANSFORMATION\",\n"
                + "                      \"description\": \"\",\n"
                + "                      \"masking\": false\n"
                + "                    }\n"
                + "                  ]\n"
                + "                }\n"
                + "              ]\n"
                + "            },\n"
                + "            \"transactiontype\": {\n"
                + "              \"inputFields\": [\n"
                + "                {\n"
                + "                  \"namespace\": \"gs://example-bucket\",\n"
                + "                  \"name\": \"warehouse/transactions\",\n"
                + "                  \"field\": \"transactiontype\",\n"
                + "                  \"transformations\": [\n"
                + "                    {\n"
                + "                      \"type\": \"DIRECT\",\n"
                + "                      \"subtype\": \"IDENTITY\",\n"
                + "                      \"description\": \"\",\n"
                + "                      \"masking\": false\n"
                + "                    }\n"
                + "                  ]\n"
                + "                }\n"
                + "              ]\n"
                + "            },\n"
                + "            \"totalamount\": {\n"
                + "              \"inputFields\": [\n"
                + "                {\n"
                + "                  \"namespace\": \"gs://example-bucket\",\n"
                + "                  \"name\": \"warehouse/transactions\",\n"
                + "                  \"field\": \"transactionamount\",\n"
                + "                  \"transformations\": [\n"
                + "                    {\n"
                + "                      \"type\": \"DIRECT\",\n"
                + "                      \"subtype\": \"AGGREGATION\",\n"
                + "                      \"description\": \"\",\n"
                + "                      \"masking\": false\n"
                + "                    }\n"
                + "                  ]\n"
                + "                }\n"
                + "              ]\n"
                + "            }\n"
                + "          },\n"
                + "          \"dataset\": [\n"
                + "            {\n"
                + "              \"namespace\": \"gs://example-bucket\",\n"
                + "              \"name\": \"warehouse/transactions\",\n"
                + "              \"field\": \"submissiondate\",\n"
                + "              \"transformations\": [\n"
                + "                {\n"
                + "                  \"type\": \"INDIRECT\",\n"
                + "                  \"subtype\": \"GROUP_BY\",\n"
                + "                  \"description\": \"\",\n"
                + "                  \"masking\": false\n"
                + "                },\n"
                + "                {\n"
                + "                  \"type\": \"INDIRECT\",\n"
                + "                  \"subtype\": \"SORT\",\n"
                + "                  \"description\": \"\",\n"
                + "                  \"masking\": false\n"
                + "                }\n"
                + "              ]\n"
                + "            },\n"
                + "            {\n"
                + "              \"namespace\": \"gs://example-bucket\",\n"
                + "              \"name\": \"warehouse/transactions\",\n"
                + "              \"field\": \"transactiontype\",\n"
                + "              \"transformations\": [\n"
                + "                {\n"
                + "                  \"type\": \"INDIRECT\",\n"
                + "                  \"subtype\": \"GROUP_BY\",\n"
                + "                  \"description\": \"\",\n"
                + "                  \"masking\": false\n"
                + "                },\n"
                + "                {\n"
                + "                  \"type\": \"INDIRECT\",\n"
                + "                  \"subtype\": \"SORT\",\n"
                + "                  \"description\": \"\",\n"
                + "                  \"masking\": false\n"
                + "                }\n"
                + "              ]\n"
                + "            }\n"
                + "          ]\n"
                + "        },\n"
                + "        \"symlinks\": {\n"
                + "          \"_producer\": \"https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/hive\",\n"
                + "          \"_schemaURL\": \"https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet\",\n"
                + "          \"identifiers\": [\n"
                + "            {\n"
                + "              \"namespace\": \"hive\",\n"
                + "              \"name\": \"default.monthly_transaction_summary\",\n"
                + "              \"type\": \"TABLE\"\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            UUID.randomUUID(), olJobNamespace, olJobName);

    return new CommandLine(new DataplexLineageCLI.DataplexCommands())
        .execute(
            "-c", "emit-ol-event",
            "-p", PROJECT_ID,
            "-l", LOCATION,
            "--event", eventJson);
  }

  @Test
  void testEmitOpenLineageEvent() {
    int exitCode = emitOpenLineageEvent();
    assertEquals(0, exitCode);
    assertTrue(outContent.toString().contains("Event emitted successfully"));
  }

  @Test
  void testGetEvents() {
    emitOpenLineageEvent();
    int exitCode =
        new CommandLine(new DataplexLineageCLI.DataplexCommands())
            .execute(
                "-c", "get-events",
                "-p", PROJECT_ID,
                "-l", LOCATION,
                "--ol-job-namespace", olJobNamespace,
                "--ol-job-name", olJobName);
    assertEquals(0, exitCode);
    JsonArray result = (JsonArray) extractLastJson();
    assertThat(result).hasSize(1);
    assertThat(result.get(0).getAsJsonObject().get("links_").getAsJsonArray()).hasSize(1);
    assertThat(
            result
                .get(0)
                .getAsJsonObject()
                .get("links_")
                .getAsJsonArray()
                .get(0)
                .getAsJsonObject()
                .get("source_")
                .getAsJsonObject()
                .get("fullyQualifiedName_")
                .getAsString())
        .isEqualTo("gcs:example-bucket.warehouse/transactions");
    assertThat(
            result
                .get(0)
                .getAsJsonObject()
                .get("links_")
                .getAsJsonArray()
                .get(0)
                .getAsJsonObject()
                .get("target_")
                .getAsJsonObject()
                .get("fullyQualifiedName_")
                .getAsString())
        .isEqualTo("dbfs:hive-lineage-cluster-m.user/hive/warehouse/monthly_transaction_summary");
  }

  @Test
  void testGetLinks() {
    emitOpenLineageEvent();
    int exitCode =
        new CommandLine(new DataplexLineageCLI.DataplexCommands())
            .execute(
                "-c", "get-links",
                "-p", PROJECT_ID,
                "-l", LOCATION,
                "--ol-job-namespace", olJobNamespace,
                "--ol-job-name", olJobName,
                "--source", "gcs:example-bucket.warehouse/transactions");
    assertEquals(0, exitCode);
    JsonArray result = (JsonArray) extractLastJson();
    assertThat(result).hasSize(1);
    assertThat(
            result
                .get(0)
                .getAsJsonObject()
                .get("source_")
                .getAsJsonObject()
                .get("fullyQualifiedName_")
                .getAsString())
        .isEqualTo("gcs:example-bucket.warehouse/transactions");
    assertThat(
            result
                .get(0)
                .getAsJsonObject()
                .get("target_")
                .getAsJsonObject()
                .get("fullyQualifiedName_")
                .getAsString())
        .isEqualTo("dbfs:hive-lineage-cluster-m.user/hive/warehouse/monthly_transaction_summary");
  }
}
