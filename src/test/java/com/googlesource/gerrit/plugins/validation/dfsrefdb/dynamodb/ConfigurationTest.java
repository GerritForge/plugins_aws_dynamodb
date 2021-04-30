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

import static com.google.common.truth.Truth.assertThat;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.Configuration.DEFAULT_LOCKS_TABLE_NAME;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.Configuration.DEFAULT_REFS_DB_TABLE_NAME;
import static org.mockito.Mockito.when;

import com.google.gerrit.server.config.PluginConfig;
import com.google.gerrit.server.config.PluginConfigFactory;
import java.net.URI;
import org.eclipse.jgit.lib.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.regions.Region;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationTest {
  private static final String PLUGIN_NAME = "plugins_aws-dynamodb";

  @Mock private PluginConfigFactory pluginConfigFactoryMock;
  private PluginConfig.Update pluginConfig;

  @Before
  public void setup() {
    pluginConfig = PluginConfig.Update.forTest(PLUGIN_NAME, new Config());
  }

  @Test
  public void shouldReadDefaultLocksTableName() {
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getLocksTableName()).isEqualTo(DEFAULT_LOCKS_TABLE_NAME);
  }

  @Test
  public void shouldReadDefaultRefsDbTableName() {
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getRefsDbTableName()).isEqualTo(DEFAULT_REFS_DB_TABLE_NAME);
  }

  @Test
  public void shouldReadConfiguredLocksTableName() {
    pluginConfig.setString("locksTableName", "foobar");
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getLocksTableName()).isEqualTo("foobar");
  }

  @Test
  public void shouldReadConfiguredRefsDbTableName() {
    pluginConfig.setString("refsDbTableName", "foobar");
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getRefsDbTableName()).isEqualTo("foobar");
  }

  @Test
  public void shouldReadEmptyEndpointByDefault() {
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getEndpoint().isPresent()).isFalse();
  }

  @Test
  public void shouldReadConfiguredEndpoint() {
    URI endpoint = URI.create("http:://localhost:4566");
    pluginConfig.setString("endpoint", endpoint.toASCIIString());
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getEndpoint().get()).isEqualTo(endpoint);
  }

  @Test
  public void shouldReadEmptyRegionByDefault() {
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getRegion().isPresent()).isFalse();
  }

  @Test
  public void shouldReadConfiguredRegion() {
    pluginConfig.setString("region", "eu-central-1");
    when(pluginConfigFactoryMock.getFromGerritConfig(PLUGIN_NAME))
        .thenReturn(pluginConfig.asPluginConfig());

    Configuration configuration = new Configuration(pluginConfigFactoryMock, PLUGIN_NAME);
    assertThat(configuration.getRegion().get()).isEqualTo(Region.EU_CENTRAL_1);
  }
}
