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

import com.google.cloud.datacatalog.lineage.v1.*;
import com.google.cloud.datacatalog.lineage.v1.LineageClient.*;
import com.google.cloud.datacatalog.lineage.v1.Process;
import java.io.IOException;
import java.util.*;

public class DataplexUtils {

  public static LineageClient getClient() throws IOException {
    LineageSettings lineageSettings = LineageSettings.newBuilder().build();
    return LineageClient.create(lineageSettings);
  }

  public static List<Link> searchLinks(LocationName locationName, String source, String target)
      throws IOException {
    try (LineageClient client = getClient()) {
      SearchLinksRequest.Builder request =
          SearchLinksRequest.newBuilder().setParent(locationName.toString());
      if (source != null) {
        request.setSource(EntityReference.newBuilder().setFullyQualifiedName(source).build());
      }
      if (target != null) {
        request.setTarget(EntityReference.newBuilder().setFullyQualifiedName(target).build());
      }
      SearchLinksPagedResponse pagedResponse = client.searchLinks(request.build());
      List<Link> links = new ArrayList<>();
      for (SearchLinksPage page : pagedResponse.iteratePages()) {
        links.addAll(page.getResponse().getLinksList());
      }
      return links;
    }
  }

  public static Process getProcessWithOLNamespace(LocationName locationName, String namespace)
      throws IOException {
    List<Process> processes = getProcessesForProject(locationName);
    for (Process process : processes) {
      if (process.getDisplayName().startsWith(namespace + ":")) {
        return process;
      }
    }
    return null;
  }

  public static Process getProcessWithDisplayName(LocationName locationName, String displayName)
      throws IOException {
    List<Process> processes = getProcessesForProject(locationName);
    for (Process process : processes) {
      if (displayName.equals(process.getDisplayName())) {
        return process;
      }
    }
    return null;
  }

  public static void deleteProcess(Process process) throws IOException {
    try (LineageClient client = getClient()) {
      DeleteProcessRequest request =
          DeleteProcessRequest.newBuilder().setName(process.getName()).build();
      client.deleteProcessOperationCallable().call(request);
    }
  }

  public static List<Process> getProcessesForProject(LocationName locationName) throws IOException {
    try (LineageClient client = getClient()) {
      ListProcessesPagedResponse pagedResponse = client.listProcesses(locationName);
      List<Process> processes = new ArrayList<>();
      for (ListProcessesPage page : pagedResponse.iteratePages()) {
        processes.addAll(page.getResponse().getProcessesList());
      }
      return processes;
    }
  }

  public static List<Run> getRunsForProject(LocationName locationName) throws IOException {
    List<Process> processes = getProcessesForProject(locationName);
    List<Run> runs = new ArrayList<>();
    for (Process process : processes) {
      runs.addAll(getRunsForProcess(process.getName()));
    }
    return runs;
  }

  public static List<LineageEvent> getEventsForProject(LocationName locationName)
      throws IOException {
    List<Run> runs = getRunsForProject(locationName);
    List<LineageEvent> events = new ArrayList<>();
    for (Run run : runs) {
      events.addAll(getEventsForRun(run.getName()));
    }
    return events;
  }

  public static Set<String> getProcessesForLink(LocationName locationName, String link)
      throws IOException {
    try (LineageClient client = getClient()) {
      BatchSearchLinkProcessesRequest request =
          BatchSearchLinkProcessesRequest.newBuilder()
              .setParent(locationName.toString())
              .addLinks(link)
              .build();
      BatchSearchLinkProcessesPagedResponse pagedResponse =
          client.batchSearchLinkProcesses(request);
      Set<String> processes = new HashSet<>();
      for (BatchSearchLinkProcessesPage page : pagedResponse.iteratePages()) {
        for (ProcessLinks links : page.getResponse().getProcessLinksList()) {
          processes.add(links.getProcess());
        }
      }
      return processes;
    }
  }

  public static List<Run> getRunsForLink(LocationName locationName, String link)
      throws IOException {
    Set<String> processes = getProcessesForLink(locationName, link);
    List<Run> runs = new ArrayList<>();
    for (String process : processes) {
      runs.addAll(getRunsForProcess(process));
    }
    return runs;
  }

  public static List<LineageEvent> getEventsForLink(LocationName locationName, String link)
      throws IOException {
    List<Run> runs = getRunsForLink(locationName, link);
    List<LineageEvent> events = new ArrayList<>();
    for (Run run : runs) {
      events.addAll(getEventsForRun(run.getName()));
    }
    return events;
  }

  public static List<Run> getRunsForProcess(String process) throws IOException {
    try (LineageClient client = getClient()) {
      ListRunsPagedResponse pagedResponse = client.listRuns(process);
      List<Run> runs = new ArrayList<>();
      for (ListRunsPage page : pagedResponse.iteratePages()) {
        runs.addAll(page.getResponse().getRunsList());
      }
      return runs;
    }
  }

  public static List<LineageEvent> getEventsForRun(String run) throws IOException {
    try (LineageClient client = getClient()) {
      ListLineageEventsPagedResponse pagedResponse = client.listLineageEvents(run);
      List<LineageEvent> events = new ArrayList<>();
      for (ListLineageEventsPage page : pagedResponse.iteratePages()) {
        events.addAll(page.getResponse().getLineageEventsList());
      }
      return events;
    }
  }

  public static List<LineageEvent> getEventsForProcess(String process) throws IOException {
    List<Run> runs = getRunsForProcess(process);
    List<LineageEvent> events = new ArrayList<>();
    for (Run run : runs) {
      events.addAll(getEventsForRun(run.getName()));
    }
    return events;
  }
}
