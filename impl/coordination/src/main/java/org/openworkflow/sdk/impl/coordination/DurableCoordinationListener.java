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
package org.openworkflow.sdk.impl.coordination;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.openworkflow.sdk.impl.WorkflowContext;
import org.openworkflow.sdk.impl.WorkflowContextData;
import org.openworkflow.sdk.impl.lifecycle.TaskCompletedEvent;
import org.openworkflow.sdk.impl.lifecycle.TaskStartedEvent;
import org.openworkflow.sdk.impl.lifecycle.WorkflowExecutionCompletableListener;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class DurableCoordinationListener implements WorkflowExecutionCompletableListener {

  static final String COORDINATION_KEY = "__coordination";

  private final Map<String, CoordinationPrimitive> primitives = new ConcurrentHashMap<>();
  private final AtomicLong globalVersion = new AtomicLong(0);
  private volatile boolean restored = false;
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private final PostPersistenceListener postPersistence = new PostPersistenceListener();

  public void register(CoordinationPrimitive primitive) {
    primitives.put(primitive.name(), primitive);
  }

  @SuppressWarnings("unchecked")
  public <T extends CoordinationPrimitive> T primitive(String name) {
    return (T) primitives.get(name);
  }

  public long currentVersion() {
    return globalVersion.get();
  }

  public WorkflowExecutionCompletableListener postPersistenceListener() {
    return postPersistence;
  }

  @Override
  public int priority() {
    return 500;
  }

  @Override
  public CompletableFuture<?> onTaskStarted(TaskStartedEvent ev) {
    restoreFromMetadata(ev.workflowContext());
    List<CompletableFuture<?>> futures = new ArrayList<>();
    for (CoordinationPrimitive p : primitives.values()) {
      if (p instanceof DurableSemaphore sem && sem.matches(ev.taskContext())) {
        futures.add(sem.acquire(ev.taskContext()));
      } else if (p instanceof DurableSignal sig && sig.matchesReceiver(ev.taskContext())) {
        futures.add(sig.await().thenApply(v -> null));
      } else if (p instanceof DurableChannel ch && ch.matchesConsumer(ev.taskContext())) {
        futures.add(ch.take(ev.taskContext()));
      } else if (p instanceof DurableChannel ch && ch.matchesProducer(ev.taskContext())) {
        futures.add(ch.put(ev.taskContext()));
      } else if (p instanceof DurableStateMachine sm && sm.matches(ev.taskContext())) {
        sm.transition(ev.taskContext());
      }
    }
    if (futures.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
  }

  @Override
  public CompletableFuture<?> onTaskCompleted(TaskCompletedEvent ev) {
    for (CoordinationPrimitive p : primitives.values()) {
      if (p instanceof DurableBarrier barrier && barrier.matches(ev.taskContext())) {
        barrier.recordArrival(ev.taskContext());
      }
      if (p instanceof DurableQuorum q && q.matches(ev.taskContext())) {
        q.complete(ev.taskContext());
      }
      if (p instanceof DurableSignal sig && sig.matchesSender(ev.taskContext())) {
        sig.send(ev.taskContext().taskName());
      }
      if (p instanceof DurableSagaTracker saga && saga.matches(ev.taskContext())) {
        saga.stepCompleted(ev.taskContext().taskName());
      }
    }
    serializeToMetadata(ev.workflowContext());
    return CompletableFuture.completedFuture(null);
  }

  @SuppressWarnings("unchecked")
  private void restoreFromMetadata(WorkflowContextData workflowContext) {
    if (restored || !(workflowContext instanceof WorkflowContext ctx)) {
      return;
    }
    var found = ctx.instance().findMetadata(COORDINATION_KEY, String.class);
    if (found.isEmpty()) {
      restored = true;
      return;
    }
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) MAPPER.readValue(found.get(), Map.class);
      CoordinationState coordState = CoordinationState.fromMap(map);
      if (coordState.version() <= globalVersion.get()) {
        restored = true;
        return;
      }
      globalVersion.set(coordState.version());
      for (var entry : coordState.primitives().entrySet()) {
        CoordinationPrimitive primitive = primitives.get(entry.getKey());
        if (primitive != null) {
          primitive.restore(entry.getValue());
        }
      }
    } catch (Exception e) {
      // skip malformed state
    }
    restored = true;
  }

  private void serializeToMetadata(WorkflowContextData workflowContext) {
    if (primitives.isEmpty() || !(workflowContext instanceof WorkflowContext ctx)) {
      return;
    }
    long version = globalVersion.incrementAndGet();
    Map<String, Map<String, Object>> serialized = new ConcurrentHashMap<>();
    for (var entry : primitives.entrySet()) {
      serialized.put(entry.getKey(), entry.getValue().serialize());
    }
    CoordinationState coordState = new CoordinationState(version, serialized);
    try {
      String json = MAPPER.writeValueAsString(coordState.toMap());
      ctx.instance().removeMetadata(COORDINATION_KEY);
      ctx.instance().addMetadataIfAbsent(COORDINATION_KEY, () -> json);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize coordination state", e);
    }
  }

  private class PostPersistenceListener implements WorkflowExecutionCompletableListener {

    @Override
    public int priority() {
      return 1500;
    }

    @Override
    public CompletableFuture<?> onTaskCompleted(TaskCompletedEvent ev) {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      for (CoordinationPrimitive p : primitives.values()) {
        if (p instanceof DurableBarrier barrier && barrier.matches(ev.taskContext())) {
          futures.add(barrier.awaitRelease());
        } else if (p instanceof DurableSemaphore sem && sem.matches(ev.taskContext())) {
          sem.release(ev.taskContext());
        }
      }
      if (futures.isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }
  }
}
