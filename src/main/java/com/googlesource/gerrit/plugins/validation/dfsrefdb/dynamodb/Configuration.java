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

import com.google.common.base.Strings;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.server.config.PluginConfig;
import com.google.gerrit.server.config.PluginConfigFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.net.URI;
import java.util.Optional;
import software.amazon.awssdk.regions.Region;

@Singleton
class Configuration {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  protected static final String DEFAULT_REFS_DB_TABLE_NAME = "refsDb";
  protected static final String DEFAULT_LOCKS_TABLE_NAME = "lockTable";
  private final Optional<Region> region;
  private final Optional<URI> endpoint;
  private final String refsDbTableName;
  private final String locksTableName;

  @Inject
  Configuration(PluginConfigFactory configFactory, @PluginName String pluginName) {
    PluginConfig pluginConfig = configFactory.getFromGerritConfig(pluginName);

    this.region = Optional.ofNullable(getStringParam(pluginConfig, "region")).map(Region::of);
    this.endpoint = Optional.ofNullable(getStringParam(pluginConfig, "endpoint")).map(URI::create);
    // TODO: add prefix
    this.refsDbTableName = pluginConfig.getString("refsDbTableName", DEFAULT_REFS_DB_TABLE_NAME);
    this.locksTableName = pluginConfig.getString("locksTableName", DEFAULT_LOCKS_TABLE_NAME);
    logger.atInfo().log(
        "dynamodb-refdb configuration: refsDbTableName: %s|locksTableName:%s%s%s",
        refsDbTableName,
        locksTableName,
        region.map(r -> String.format("|region: %s", r.id())).orElse(""),
        endpoint.map(e -> String.format("|endpoint: %s", e.toASCIIString())).orElse(""));
  }

  Optional<Region> getRegion() {
    return region;
  }

  Optional<URI> getEndpoint() {
    return endpoint;
  }

  private static String getStringParam(PluginConfig pluginConfig, String name) {
    return Strings.isNullOrEmpty(System.getProperty(name))
        ? pluginConfig.getString(name)
        : System.getProperty(name);
  }

  String getRefsDbTableName() {
    return refsDbTableName;
  }

  String getLocksTableName() {
    return locksTableName;
  }
}
