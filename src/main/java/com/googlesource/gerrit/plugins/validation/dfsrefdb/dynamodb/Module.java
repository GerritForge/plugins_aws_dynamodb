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

import static com.google.inject.Scopes.SINGLETON;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.gerritforge.gerrit.globalrefdb.GlobalRefDatabase;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.lifecycle.LifecycleModule;
import com.google.inject.Scopes;

class Module extends LifecycleModule {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  @Override
  protected void configure() {
    logger.atInfo().log("Shared ref-db engine: DynamoDB");
    DynamicItem.bind(binder(), GlobalRefDatabase.class)
        .to(DynamoDBRefDatabase.class)
        .in(Scopes.SINGLETON);
    bind(AmazonDynamoDB.class).toProvider(AmazonDynamoDBProvider.class).in(SINGLETON);
    bind(AmazonDynamoDBLockClient.class).toProvider(DynamoDBLockClientProvider.class).in(SINGLETON);
    listener().to(DynamoDBLifeCycleManager.class);
  }
}
