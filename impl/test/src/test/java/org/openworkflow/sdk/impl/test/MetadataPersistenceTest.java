/*
 * Copyright 2020-Present The Open Workflow Specification Authors
 *
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
package org.openworkflow.sdk.impl.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.openworkflow.sdk.fluent.spec.dsl.DSL.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openworkflow.sdk.api.types.Workflow;
import org.openworkflow.sdk.fluent.spec.WorkflowBuilder;
import org.openworkflow.sdk.impl.WorkflowApplication;
import org.openworkflow.sdk.impl.WorkflowContext;
import org.openworkflow.sdk.impl.WorkflowDefinition;
import org.openworkflow.sdk.impl.WorkflowInstance;
import org.openworkflow.sdk.impl.WorkflowStatus;
import org.openworkflow.sdk.impl.lifecycle.TaskCompletedEvent;
import org.openworkflow.sdk.impl.lifecycle.TaskStartedEvent;
import org.openworkflow.sdk.impl.lifecycle.WorkflowExecutionCompletableListener;
import org.openworkflow.sdk.impl.persistence.DefaultPersistenceInstanceHandlers;
import org.openworkflow.sdk.impl.persistence.PersistenceApplicationBuilder;
import org.openworkflow.sdk.impl.persistence.PersistenceInstanceHandlers;
import org.openworkflow.sdk.impl.persistence.mvstore.MVStorePersistenceStore;

@DisplayName("Metadata persistence round-trip")
class MetadataPersistenceTest {

  private static final String APP_ID = "meta-test";

  @TempDir Path tempDir;

  @Test
  @DisplayName("String metadata survives crash/restore")
  void stringMetadataSurvivesCrashRestore() throws Exception {
    String dbPath = tempDir.resolve("meta-test.db").toString();
    CountDownLatch step2Done = new CountDownLatch(1);

    Workflow workflow =
        WorkflowBuilder.workflow("meta-test", "test", "0.1.0")
            .tasks(
                doTasks(
                    set("step1", s -> s.put("a", "1")),
                    set("step2", s -> s.put("b", "2")),
                    set("step3", s -> s.put("c", "3"))))
            .build();

    WorkflowExecutionCompletableListener metaSetter =
        new WorkflowExecutionCompletableListener() {
          @Override
          public int priority() {
            return 500;
          }

          @Override
          public CompletableFuture<?> onTaskCompleted(TaskCompletedEvent ev) {
            WorkflowContext ctx = (WorkflowContext) ev.workflowContext();
            String taskName = ev.taskContext().taskName();
            ctx.instance().removeMetadata("counter");
            int count = taskName.equals("step1") ? 1 : taskName.equals("step2") ? 2 : 3;
            ctx.instance().addMetadataIfAbsent("counter", () -> String.valueOf(count));
            System.out.println(
                "SET: counter="
                    + count
                    + " task="
                    + taskName
                    + " keys="
                    + ctx.instance().metadata().keySet()
                    + " val="
                    + ctx.instance().metadata());
            if (taskName.equals("step2")) {
              step2Done.countDown();
            }
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletableFuture<?> onTaskStarted(TaskStartedEvent ev) {
            if (ev.taskContext().taskName().equals("step3")) {
              return new CompletableFuture<>();
            }
            return CompletableFuture.completedFuture(null);
          }
        };

    ExecutorService exec1 = Executors.newSingleThreadExecutor();
    PersistenceInstanceHandlers handlers1 =
        DefaultPersistenceInstanceHandlers.builder(new MVStorePersistenceStore(dbPath))
            .withExecutorService(exec1)
            .build();

    WorkflowApplication app1 =
        PersistenceApplicationBuilder.builder(
                WorkflowApplication.builder().withId(APP_ID).withListener(metaSetter),
                handlers1.writer())
            .build();

    app1.workflowDefinition(workflow).instance(Map.of()).start();
    assertThat(step2Done.await(5, TimeUnit.SECONDS)).as("step2 completed").isTrue();
    Thread.sleep(200);

    app1.close();
    handlers1.close();
    exec1.shutdownNow();

    StringBuilder restoredValue = new StringBuilder();

    WorkflowExecutionCompletableListener restoreChecker =
        new WorkflowExecutionCompletableListener() {
          @Override
          public int priority() {
            return 500;
          }

          @Override
          public CompletableFuture<?> onTaskStarted(TaskStartedEvent ev) {
            WorkflowContext ctx = (WorkflowContext) ev.workflowContext();
            System.out.println(
                "RESTORE onTaskStarted: task="
                    + ev.taskContext().taskName()
                    + " keys="
                    + ctx.instance().metadata().keySet()
                    + " metadata="
                    + ctx.instance().metadata());
            var found = ctx.instance().findMetadata("counter", String.class);
            System.out.println("RESTORE findMetadata counter=" + found);
            found.ifPresent(restoredValue::append);
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletableFuture<?> onTaskCompleted(TaskCompletedEvent ev) {
            return CompletableFuture.completedFuture(null);
          }
        };

    ExecutorService exec2 = Executors.newSingleThreadExecutor();
    PersistenceInstanceHandlers handlers2 =
        DefaultPersistenceInstanceHandlers.builder(new MVStorePersistenceStore(dbPath))
            .withExecutorService(exec2)
            .build();

    WorkflowApplication app2 =
        PersistenceApplicationBuilder.builder(
                WorkflowApplication.builder().withId(APP_ID).withListener(restoreChecker),
                handlers2.writer())
            .build();

    WorkflowDefinition def2 = app2.workflowDefinition(workflow);
    try (Stream<WorkflowInstance> stream = handlers2.reader().scanAll(def2)) {
      List<WorkflowInstance> instances = stream.toList();
      System.out.println("RESTORE: found " + instances.size() + " instances");
      assertThat(instances).as("Should find 1 persisted instance").hasSize(1);

      WorkflowInstance restored = instances.get(0);
      System.out.println("RESTORE before start: metadata=" + restored.metadata());
      restored.start();

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> restored.status() == WorkflowStatus.COMPLETED);

      System.out.println("RESTORE after complete: metadata=" + restored.metadata());
    } finally {
      app2.close();
      handlers2.close();
      exec2.shutdownNow();
    }

    System.out.println("RESULT: restoredValue=" + restoredValue);
    assertThat(restoredValue.toString())
        .as("Metadata counter should be restored from pre-crash state")
        .isNotEmpty();
  }

  @Test
  @DisplayName("Fork metadata survives crash/restore")
  void forkMetadataSurvivesCrashRestore() throws Exception {
    String dbPath = tempDir.resolve("fork-meta.db").toString();
    CountDownLatch twoDone = new CountDownLatch(2);

    Workflow workflow =
        WorkflowBuilder.workflow("fork-meta", "test", "0.1.0")
            .tasks(
                doTasks(
                    fork(
                        "parallel",
                        f ->
                            f.compete(false)
                                .branch("fast", b -> b.set("workFast", s -> s.put("done", "fast")))
                                .branch(
                                    "medium",
                                    b ->
                                        b.wait("d1", w -> w.wait(Duration.ofMillis(50)))
                                            .set("workMedium", s -> s.put("done", "medium")))
                                .branch(
                                    "slow",
                                    b ->
                                        b.wait("d2", w -> w.wait(Duration.ofMillis(100)))
                                            .set("workSlow", s -> s.put("done", "slow"))))))
            .build();

    WorkflowExecutionCompletableListener metaSetter =
        new WorkflowExecutionCompletableListener() {
          @Override
          public int priority() {
            return 500;
          }

          @Override
          public CompletableFuture<?> onTaskCompleted(TaskCompletedEvent ev) {
            WorkflowContext ctx = (WorkflowContext) ev.workflowContext();
            String taskName = ev.taskContext().taskName();
            if (taskName.startsWith("work")) {
              ctx.instance().removeMetadata("arrivals");
              String current = ctx.instance().findMetadata("arrivals", String.class).orElse("");
              String updated = current.isEmpty() ? taskName : current + "," + taskName;
              ctx.instance().addMetadataIfAbsent("arrivals", () -> updated);
              System.out.println("FORK SET: arrivals=" + updated + " task=" + taskName);
              twoDone.countDown();
            }
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletableFuture<?> onTaskStarted(TaskStartedEvent ev) {
            if (ev.taskContext().taskName().equals("workSlow")) return new CompletableFuture<>();
            return CompletableFuture.completedFuture(null);
          }
        };

    ExecutorService exec1 = Executors.newSingleThreadExecutor();
    PersistenceInstanceHandlers handlers1 =
        DefaultPersistenceInstanceHandlers.builder(new MVStorePersistenceStore(dbPath))
            .withExecutorService(exec1)
            .build();
    WorkflowApplication app1 =
        PersistenceApplicationBuilder.builder(
                WorkflowApplication.builder().withId(APP_ID).withListener(metaSetter),
                handlers1.writer())
            .build();
    app1.workflowDefinition(workflow).instance(Map.of()).start();
    assertThat(twoDone.await(5, TimeUnit.SECONDS)).as("two work tasks done").isTrue();
    Thread.sleep(200);
    app1.close();
    handlers1.close();
    exec1.shutdownNow();

    StringBuilder restoredArrivals = new StringBuilder();
    WorkflowExecutionCompletableListener restoreChecker =
        new WorkflowExecutionCompletableListener() {
          @Override
          public int priority() {
            return 500;
          }

          @Override
          public CompletableFuture<?> onTaskStarted(TaskStartedEvent ev) {
            WorkflowContext ctx = (WorkflowContext) ev.workflowContext();
            System.out.println(
                "FORK RESTORE onTaskStarted: task="
                    + ev.taskContext().taskName()
                    + " keys="
                    + ctx.instance().metadata().keySet()
                    + " arrivals="
                    + ctx.instance().findMetadata("arrivals", String.class));
            ctx.instance()
                .findMetadata("arrivals", String.class)
                .ifPresent(
                    v -> {
                      if (restoredArrivals.isEmpty()) restoredArrivals.append(v);
                    });
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletableFuture<?> onTaskCompleted(TaskCompletedEvent ev) {
            return CompletableFuture.completedFuture(null);
          }
        };

    ExecutorService exec2 = Executors.newSingleThreadExecutor();
    PersistenceInstanceHandlers handlers2 =
        DefaultPersistenceInstanceHandlers.builder(new MVStorePersistenceStore(dbPath))
            .withExecutorService(exec2)
            .build();
    WorkflowApplication app2 =
        PersistenceApplicationBuilder.builder(
                WorkflowApplication.builder().withId(APP_ID).withListener(restoreChecker),
                handlers2.writer())
            .build();
    WorkflowDefinition def2 = app2.workflowDefinition(workflow);
    try (Stream<WorkflowInstance> stream = handlers2.reader().scanAll(def2)) {
      List<WorkflowInstance> instances = stream.toList();
      System.out.println("FORK RESTORE: found " + instances.size() + " instances");
      assertThat(instances).hasSize(1);
      WorkflowInstance restored = instances.get(0);
      restored.start();
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> restored.status() == WorkflowStatus.COMPLETED);
      System.out.println("FORK RESTORE complete: arrivals=" + restoredArrivals);
    } finally {
      app2.close();
      handlers2.close();
      exec2.shutdownNow();
    }

    assertThat(restoredArrivals.toString())
        .as("Fork metadata should survive crash/restore")
        .isNotEmpty();
  }
}
