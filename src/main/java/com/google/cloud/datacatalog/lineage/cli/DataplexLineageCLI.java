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

import com.google.cloud.datacatalog.lineage.v1.LineageEvent;
import com.google.cloud.datacatalog.lineage.v1.Link;
import com.google.cloud.datacatalog.lineage.v1.LocationName;
import com.google.cloud.datacatalog.lineage.v1.Process;
import com.google.cloud.datacatalog.lineage.v1.Run;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

public class DataplexLineageCLI {

  @Command(
      name = "dataplex-lineage-cli",
      mixinStandardHelpOptions = true,
      version = "1.0-SNAPSHOT",
      description = "CLI for Dataplex Lineage operations")
  static class DataplexCommands implements Callable<Integer> {

    @Option(
        names = {"-c", "--command"},
        description =
            "Command to execute: 'emit-ol-event', 'search-links', 'get-processes', 'get-runs', 'get-events', 'get-processes-for-link', 'get-runs-for-link', 'get-events-for-link'",
        required = true)
    private String command;

    @Option(
        names = {"-p", "--project"},
        description = "Google Cloud project ID",
        required = true)
    private String project;

    @Option(
        names = {"-l", "--location"},
        description = "Location (defaults to 'us')")
    private String location = "us";

    @Option(
        names = {"--credentials-file"},
        description = "Path to the credentials file")
    private String credentialsFile;

    @Option(
        names = {"--endpoint"},
        description = "API endpoint")
    private String endpoint;

    @Option(
        names = {"--mode"},
        description = "Operation mode: 'sync' or 'async' (for emit-ol-event)")
    private DataplexConfig.Mode mode = DataplexConfig.Mode.sync;

    @Option(
        names = {"--event"},
        description = "JSON string of the event to emit")
    private String eventJson;

    @Option(
        names = {"--source"},
        description = "Source for link search")
    private String source;

    @Option(
        names = {"--target"},
        description = "Target for link search")
    private String target;

    @Option(
        names = {"--ol-job-namespace"},
        description = "OpenLineage job namespace")
    private String olJobNamespace;

    @Option(
        names = {"--ol-job-name"},
        description = "OpenLineage job name")
    private String olJobName;

    @Option(
        names = {"--process"},
        description = "Process name")
    private String process;

    @Option(
        names = {"--run"},
        description = "Run name")
    private String run;

    @Option(
        names = {"--link"},
        description = "Link for processes, runs, or events queries")
    private String link;

    @Option(
        names = {"--pretty"},
        description = "Pretty print JSON output")
    private boolean pretty = false;

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public Integer call() throws Exception {
      LocationName locationName = LocationName.of(project, location);

      try {
        switch (command) {
          case "emit-ol-event":
            emitEvent(locationName);
            break;
          case "search-links":
            searchLinks(locationName);
            break;
          case "get-processes":
            getProcesses(locationName);
            break;
          case "get-runs":
            getRuns(locationName);
            break;
          case "get-events":
            getEvents(locationName);
            break;
          case "get-processes-for-link":
            getProcessesForLink(locationName);
            break;
          case "get-runs-for-link":
            getRunsForLink(locationName);
            break;
          case "get-events-for-link":
            getEventsForLink(locationName);
            break;
          default:
            System.err.println("Unknown command: " + command);
            return 1;
        }
      } catch (Exception e) {
        System.err.println("Error executing command: " + e.getMessage());
        e.printStackTrace();
        return 1;
      }

      return 0;
    }

    private void emitEvent(LocationName locationName) throws IOException {
      if (eventJson == null) {
        System.err.println("Event JSON is required for 'emit-ol-event' command");
        return;
      }
      try {
        JsonObject jsonObject = JsonParser.parseString(eventJson).getAsJsonObject();
        DataplexConfig config =
            new DataplexConfig(endpoint, project, credentialsFile, location, mode);
        try (DataplexTransport transport =
            new DataplexTransport(new DataplexTransport.ProducerClientWrapper(config))) {
          transport.getProducerClientWrapper().emitEvent(gson.toJson(jsonObject));
          System.out.println("Event emitted successfully");
        }
      } catch (Exception e) {
        System.err.println("Error parsing or emitting event JSON: " + e.getMessage());
      }
    }

    private void searchLinks(LocationName locationName) throws IOException {
      List<Link> links = DataplexUtils.searchLinks(locationName, source, target);
      System.out.println(gson.toJson(links));
    }

    private void getProcesses(LocationName locationName) throws IOException {
      List<Process> processes = DataplexUtils.getProcessesForProject(locationName);
      System.out.println(gson.toJson(processes));
    }

    private void getRuns(LocationName locationName) throws IOException {
      List<Run> runs;
      if (process != null) {
        runs = DataplexUtils.getRunsForProcess(process);
      } else {
        runs = DataplexUtils.getRunsForProject(locationName);
      }
      System.out.println(gson.toJson(runs));
    }

    private void getEvents(LocationName locationName) throws IOException {
      List<LineageEvent> events;
      if (run != null) {
        events = DataplexUtils.getEventsForRun(run);
      } else if (process != null) {
        events = DataplexUtils.getEventsForProcess(process);
      } else if (olJobNamespace != null && olJobName != null) {
        Process process =
            DataplexUtils.getProcessWithDisplayName(
                locationName, String.format("%s:%s", olJobNamespace, olJobName));
        if (process == null) {
          System.err.println("Process not found");
          return;
        }
        events = DataplexUtils.getEventsForProcess(process.getName());
      } else {
        events = DataplexUtils.getEventsForProject(locationName);
      }
      System.out.println(gson.toJson(events));
    }

    private void getProcessesForLink(LocationName locationName) throws IOException {
      if (link == null) {
        System.err.println("Link is required for 'get-processes-for-link' command");
        return;
      }
      Set<String> processes = DataplexUtils.getProcessesForLink(locationName, link);
      System.out.println(gson.toJson(processes));
    }

    private void getRunsForLink(LocationName locationName) throws IOException {
      if (link == null) {
        System.err.println("Link is required for 'get-runs-for-link' command");
        return;
      }
      List<Run> runs = DataplexUtils.getRunsForLink(locationName, link);
      System.out.println(gson.toJson(runs));
    }

    private void getEventsForLink(LocationName locationName) throws IOException {
      if (link == null) {
        System.err.println("Link is required for 'get-events-for-link' command");
        return;
      }
      List<LineageEvent> events = DataplexUtils.getEventsForLink(locationName, link);
      System.out.println(gson.toJson(events));
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new DataplexCommands()).execute(args);
    System.exit(exitCode);
  }
}
