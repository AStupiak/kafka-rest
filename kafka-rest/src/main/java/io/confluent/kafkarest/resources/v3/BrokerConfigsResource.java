/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.BrokerConfigManager;
import io.confluent.kafkarest.entities.BrokerConfig;
import io.confluent.kafkarest.entities.v3.BrokerConfigData;
import io.confluent.kafkarest.entities.v3.BrokerData;
import io.confluent.kafkarest.entities.v3.ClusterData;
import io.confluent.kafkarest.entities.v3.CollectionLink;
import io.confluent.kafkarest.entities.v3.GetBrokerConfigResponse;
import io.confluent.kafkarest.entities.v3.ListBrokerConfigsResponse;
import io.confluent.kafkarest.entities.v3.ResourceLink;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/v3/clusters/{clusterId}/brokers/{brokerId}/configs")
public final class BrokerConfigsResource {

  private final BrokerConfigManager brokerConfigManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public BrokerConfigsResource(
      BrokerConfigManager brokerConfigManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory) {
    this.brokerConfigManager = Objects.requireNonNull(brokerConfigManager);
    this.crnFactory = Objects.requireNonNull(crnFactory);
    this.urlFactory = Objects.requireNonNull(urlFactory);
  }

  @GET
  @Produces(Versions.JSON_API)
  public void listBrokerConfigs(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") int brokerId) {
    CompletableFuture<ListBrokerConfigsResponse> response =
        brokerConfigManager.listBrokerConfigs(clusterId, brokerId)
            .thenApply(
                configs ->
                    new ListBrokerConfigsResponse(
                        new CollectionLink(
                            urlFactory.create(
                                "v3", "clusters", clusterId, "brokers", String.valueOf(brokerId),
                                "configs"),
                            /* next= */null),
                        configs.stream()
                            .sorted(Comparator.comparing(BrokerConfig::getName))
                            .map(this::toBrokerConfigData)
                            .collect(Collectors.toList())));
    AsyncResponses.asyncResume(asyncResponse, response);
  }

  @GET
  @Path("/{name}")
  @Produces(Versions.JSON_API)
  public void getBrokerConfig(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("brokerId") int brokerId,
      @PathParam("name") String name
  ) {
    CompletableFuture<GetBrokerConfigResponse> response =
        brokerConfigManager.getBrokerConfig(clusterId, brokerId, name)
            .thenApply(broker -> broker.orElseThrow(NotFoundException::new))
            .thenApply(broker -> new GetBrokerConfigResponse(toBrokerConfigData(broker)));

    AsyncResponses.asyncResume(asyncResponse, response);
  }

  private BrokerConfigData toBrokerConfigData(BrokerConfig brokerConfig) {
    return new BrokerConfigData(
        crnFactory.create(
            ClusterData.ELEMENT_TYPE,
            brokerConfig.getClusterId(),
            BrokerData.ELEMENT_TYPE,
            String.valueOf(brokerConfig.getBrokerId()),
            BrokerConfigData.ELEMENT_TYPE,
            brokerConfig.getName()),
        new ResourceLink(
            urlFactory.create(
                "v3",
                "clusters",
                brokerConfig.getClusterId(),
                "brokers",
                String.valueOf(brokerConfig.getBrokerId()),
                "configs",
                brokerConfig.getName())),
        brokerConfig.getClusterId(),
        brokerConfig.getBrokerId(),
        brokerConfig.getName(),
        brokerConfig.getValue(),
        brokerConfig.isDefault(),
        brokerConfig.isReadOnly(),
        brokerConfig.isSensitive());
  }
}
