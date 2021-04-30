// Copyright (C) 2021 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import software.amazon.awssdk.regions.Region;

@Singleton
class AmazonDynamoDBProvider implements Provider<AmazonDynamoDB> {
  private final Configuration configuration;

  @Inject
  AmazonDynamoDBProvider(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public AmazonDynamoDB get() {
    AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
    String region =
        configuration
            .getRegion()
            .map(Region::id)
            .orElseGet(() -> new DefaultAwsRegionProviderChain().getRegion());

    configuration
        .getEndpoint()
        .ifPresent(
            endpoint ->
                builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint.toASCIIString(), region)));
    return builder.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
  }
}
