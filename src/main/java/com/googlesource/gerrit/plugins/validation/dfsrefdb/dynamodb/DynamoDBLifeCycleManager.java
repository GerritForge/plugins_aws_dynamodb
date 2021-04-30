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

import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.DynamoDBRefDatabase.LOCK_DB_PRIMARY_KEY;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.DynamoDBRefDatabase.LOCK_DB_SORT_KEY;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.DynamoDBRefDatabase.REF_DB_PRIMARY_KEY;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.CreateDynamoDBTableOptions;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
class DynamoDBLifeCycleManager implements LifecycleListener {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final Configuration configuration;
  private final AmazonDynamoDB dynamoDB;

  @Inject
  DynamoDBLifeCycleManager(Configuration configuration, AmazonDynamoDB dynamoDB) {
    this.configuration = configuration;
    this.dynamoDB = dynamoDB;
  }

  // TODO: it is useful to create these at start up during development
  // however ddb tables should be created beforehand, because it might take a while.
  // Perhaps it'd be useful to move this logic to an SSH command.

  @Override
  public void start() {
    createLockTableIfDoesntExist();
    createRefsDbTableIfDoesntExist();
  }

  @Override
  public void stop() {}

  private void createLockTableIfDoesntExist() {
    if (!tableExists(dynamoDB, configuration.getLocksTableName())) {
      logger.atWarning().log(
          "Attempt to create lock table '%s'", configuration.getLocksTableName());
      AmazonDynamoDBLockClient.createLockTableInDynamoDB(
          CreateDynamoDBTableOptions.builder(
                  dynamoDB, new ProvisionedThroughput(10L, 10L), configuration.getLocksTableName())
              .withPartitionKeyName(LOCK_DB_PRIMARY_KEY)
              .withSortKeyName(LOCK_DB_SORT_KEY)
              .build());

      try {
        logger.atWarning().log(
            "Wait for lock table '%s' creation", configuration.getLocksTableName());
        TableUtils.waitUntilActive(dynamoDB, configuration.getLocksTableName());
        logger.atWarning().log(
            "lock table '%s' successfully created and active", configuration.getLocksTableName());
      } catch (InterruptedException e) {
        logger.atSevere().withCause(e).log(
            "Timeout when creating lock table '%s'", configuration.getLocksTableName());
      }
    } else {
      logger.atWarning().log(
          "Lock table '%s' already exists, nothing to do.", configuration.getLocksTableName());
    }
  }

  private void createRefsDbTableIfDoesntExist() {
    boolean created =
        TableUtils.createTableIfNotExists(
            dynamoDB,
            new CreateTableRequest()
                .withTableName(configuration.getRefsDbTableName())
                .withAttributeDefinitions(
                    new AttributeDefinition(REF_DB_PRIMARY_KEY, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(REF_DB_PRIMARY_KEY, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L)));

    if (created) {
      try {
        logger.atWarning().log(
            "Wait for refsDB table '%s' creation", configuration.getRefsDbTableName());
        TableUtils.waitUntilActive(dynamoDB, configuration.getRefsDbTableName());
        logger.atWarning().log(
            "refsDb table '%s' successfully created and active",
            configuration.getRefsDbTableName());
      } catch (InterruptedException e) {
        logger.atSevere().withCause(e).log(
            "Timeout when creating refsDb table '%s'", configuration.getRefsDbTableName());
      }
    } else {
      logger.atWarning().log(
          "RefsDb table '%s' already exists, nothing to do.", configuration.getRefsDbTableName());
    }
  }

  @VisibleForTesting
  static boolean tableExists(AmazonDynamoDB dynamoDB, String tableName) {
    final Table table = new Table(dynamoDB, tableName);
    try {
      table.describe();
    } catch (ResourceNotFoundException e) {
      return false;
    }
    return true;
  }
}
