/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.admin.v2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalableTopicsDeleteTest {
    @DataProvider
    public Object[][] closeResults() {
        return new Object[][] {{true, false}, {true, true}, {false, false}};
    }

    @Test(dataProvider = "closeResults")
    public void testDeleteWaitsForControllerClose(boolean serviceAvailable, boolean failClose) {
        PulsarService pulsar = mock(PulsarService.class);
        PulsarResources pulsarResources = mock(PulsarResources.class);
        ScalableTopicResources resources = mock(ScalableTopicResources.class);
        BrokerService broker = mock(BrokerService.class);
        ScalableTopicService service = mock(ScalableTopicService.class);
        MetadataStore store = mock(MetadataStore.class);
        TopicName topic = TopicName.get("topic://tenant/ns/topic");
        CompletableFuture<Void> closed = new CompletableFuture<>();
        when(pulsar.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarResources.getScalableTopicResources()).thenReturn(resources);
        when(pulsar.getBrokerService()).thenReturn(broker);
        when(broker.getScalableTopicService()).thenReturn(serviceAvailable ? service : null);
        when(resources.getStore()).thenReturn(store);
        when(resources.controllerLockPath(topic)).thenReturn("/topics/tenant/ns/topic/controller");
        when(store.get("/topics/tenant/ns/topic/controller"))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(resources.getScalableTopicMetadataAsync(topic))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(ScalableTopicMetadata.builder().build())));
        when(service.releaseController(topic)).thenReturn(closed);
        when(resources.deleteScalableTopicAsync(topic)).thenReturn(CompletableFuture.completedFuture(null));
        ScalableTopics resource = new ScalableTopics() {
            @Override
            protected PulsarService pulsar() {
                return pulsar;
            }

            @Override
            public String clientAppId() {
                return "test-client";
            }

            @Override
            public CompletableFuture<Void> validateNamespaceOperationAsync(NamespaceName namespace,
                                                                           NamespaceOperation operation) {
                return CompletableFuture.completedFuture(null);
            }
        };
        AsyncResponse response = mock(AsyncResponse.class);
        resource.deleteScalableTopic(response, "tenant", "ns", "topic", true);
        if (!serviceAvailable) {
            verifyNoInteractions(service);
            verify(resources).deleteScalableTopicAsync(topic);
            verify(response).resume(any(Response.class));
            return;
        }
        verify(service).releaseController(topic);
        verify(resources, never()).deleteScalableTopicAsync(topic);
        verifyNoInteractions(response);

        if (failClose) {
            closed.completeExceptionally(new IllegalStateException("close failed"));
            verify(resources, never()).deleteScalableTopicAsync(topic);
            verify(response).resume(any(Throwable.class));
        } else {
            closed.complete(null);
            verify(resources).deleteScalableTopicAsync(topic);
            verify(response).resume(any(Response.class));
        }
    }
}
