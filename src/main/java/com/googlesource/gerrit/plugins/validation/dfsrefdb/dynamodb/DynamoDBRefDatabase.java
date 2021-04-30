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

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.gerritforge.gerrit.globalrefdb.GlobalRefDatabase;
import com.gerritforge.gerrit.globalrefdb.GlobalRefDbLockException;
import com.gerritforge.gerrit.globalrefdb.GlobalRefDbSystemError;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.entities.Project;
import com.google.inject.Inject;
import java.util.Optional;
import javax.inject.Singleton;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;

@Singleton
public class DynamoDBRefDatabase implements GlobalRefDatabase {

  public static final String REF_DB_PRIMARY_KEY = "refPath";
  public static final String REF_DB_VALUE_KEY = "refValue";

  public static final String LOCK_DB_PRIMARY_KEY = "lockKey";
  public static final String LOCK_DB_SORT_KEY = "lockValue";

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final AmazonDynamoDBLockClient lockClient;
  private final AmazonDynamoDB dynamoDBClient;
  private final Configuration configuration;

  @Inject
  DynamoDBRefDatabase(
      AmazonDynamoDBLockClient lockClient,
      AmazonDynamoDB dynamoDBClient,
      Configuration configuration) {
    this.lockClient = lockClient;
    this.dynamoDBClient = dynamoDBClient;
    this.configuration = configuration;
  }

  static String pathFor(Project.NameKey projectName, String refName) {
    return "/" + projectName + "/" + refName;
  }

  @Override
  public boolean isUpToDate(Project.NameKey project, Ref ref) throws GlobalRefDbLockException {
    try {
      GetItemResult result = getPathFromDynamoDB(project, ref.getName());
      if (!exists(result)) {
        return true;
      }

      String valueInDynamoDB = result.getItem().get(REF_DB_VALUE_KEY).getS();
      // Assuming this is a delete node NULL_REF
      if (valueInDynamoDB == null) {
        logger.atFine().log(
            "%s:%s not found in dynamoDB, assumed as delete node NULL_REF", project, ref.getName());
        return false;
      }

      ObjectId objectIdInSharedRefDb = ObjectId.fromString(valueInDynamoDB);
      boolean isUpToDate = objectIdInSharedRefDb.equals(ref.getObjectId());

      if (!isUpToDate) {
        logger.atWarning().log(
            "%s:%s is out of sync: local=%s dynamodb=%s",
            project, ref.getName(), ref.getObjectId(), objectIdInSharedRefDb);
      }
      return isUpToDate;
    } catch (Exception e) {
      throw new GlobalRefDbLockException(project.get(), ref.getName(), e);
    }
  }

  @Override
  public boolean compareAndPut(Project.NameKey project, Ref currRef, ObjectId newRefValue)
      throws GlobalRefDbSystemError {
    String refPath = pathFor(project, currRef.getName());
    ObjectId newValue = newRefValue == null ? ObjectId.zeroId() : newRefValue;

    return compareAndPut(project, refPath, currRef.getObjectId().getName(), newValue.getName());
  }

  @Override
  public <T> boolean compareAndPut(Project.NameKey project, String refName, T currValue, T newValue)
      throws GlobalRefDbSystemError {
    String refPath = pathFor(project, refName);

    String newRefValue = newValue == null ? ObjectId.zeroId().getName() : newValue.toString();
    String curRefValue = currValue == null ? ObjectId.zeroId().getName() : currValue.toString();

    return compareAndPut(project, refPath, curRefValue, newRefValue);
  }

  private boolean compareAndPut(
      Project.NameKey project, String refPath, String currValueForPath, String newValueForPath)
      throws GlobalRefDbSystemError {

    // awslocal dynamodb update-item --table-name refsDb --key '{"refpath": {"S": "test"}}'
    //  --update-expression 'SET #REF_VALUE = :new_h'
    //  --expression-attribute-names '{"#REF_VALUE":"refValue"}'
    //  --expression-attribute-values
    // '{":new_h":{"S":"new_value_1"},":old_h":{"S":"updated_value_123"}}'
    //  --return-values UPDATED_NEW

    UpdateItemRequest updateItemRequest =
        new UpdateItemRequest()
            .withTableName(configuration.getRefsDbTableName())
            .withKey(ImmutableMap.of(REF_DB_PRIMARY_KEY, new AttributeValue(refPath)))
            .withExpressionAttributeValues(
                ImmutableMap.of(
                    ":old_value", new AttributeValue(currValueForPath),
                    ":new_value", new AttributeValue(newValueForPath)))
            .withUpdateExpression(String.format("SET %s = %s", REF_DB_VALUE_KEY, ":new_value"))
            .withConditionExpression(
                String.format(
                    "attribute_not_exists(%s) OR %s = :old_value",
                    REF_DB_PRIMARY_KEY, REF_DB_VALUE_KEY))
            .withReturnValues(ReturnValue.ALL_OLD);

    // Run the transaction and process the result.
    try {
      UpdateItemResult updateItemResult = dynamoDBClient.updateItem(updateItemRequest);
      logger.atFine().log(
          "Updated path for project %s. Current: %s New: %s",
          project.get(), currValueForPath, newValueForPath);
      return true;
    } catch (ConditionalCheckFailedException e) {
      throw new GlobalRefDbSystemError(
          String.format(
              "Conditional Check Failure when updating refPath %s. expected: %s New: %s",
              refPath, currValueForPath, newValueForPath),
          e);
    } catch (Exception e) {
      throw new GlobalRefDbSystemError(
          String.format(
              "Error updating refPath %s. expected: %s new: %s",
              project.get(), currValueForPath, newValueForPath),
          e);
    }
  }

  @Override
  public AutoCloseable lockRef(Project.NameKey project, String refName)
      throws GlobalRefDbLockException {
    String refPath = pathFor(project, refName);
    try {
      // Attempts to acquire a lock until it either acquires the lock, or a specified
      // additionalTimeToWaitForLock is reached.
      // TODO: 'additionalTimeToWaitForLock' should be configurable
      LockItem lockItem =
          lockClient.acquireLock(AcquireLockOptions.builder(refPath).withSortKey(refPath).build());
      logger.atFine().log("Acquired lock for %s", refPath);
      return lockItem;
    } catch (InterruptedException e) {
      logger.atSevere().withCause(e).log(
          "Received interrupted signal when trying to acquire lock for %s", refPath);
      throw new GlobalRefDbLockException(project.get(), refName, e);
    } catch (LockNotGrantedException e) {
      logger.atSevere().withCause(e).log("Failed to acquire lock for %s", refPath);
      throw new GlobalRefDbLockException(project.get(), refName, e);
    }
  }

  @Override
  public boolean exists(Project.NameKey project, String refName) {
    try {
      if (!exists(getPathFromDynamoDB(project, refName))) {
        logger.atFine().log("ref '%s' does not exist in dynamodb", pathFor(project, refName));
        return false;
      }
      return true;

    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "Could not check for '%s' existence", pathFor(project, refName));
    }

    return false;
  }

  @Override
  public void remove(Project.NameKey project) throws GlobalRefDbSystemError {
    // TODO: to remove all refs related to project we'd need to be able to query
    // dynamodb by 'project': perhaps we should have a composite key of:
    // PK: project, SK: ref
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Optional<T> get(Project.NameKey project, String refName, Class<T> clazz)
      throws GlobalRefDbSystemError {
    try {
      GetItemResult item = getPathFromDynamoDB(project, refName);
      if (!exists(item)) {
        return Optional.empty();
      }
      String refValue = item.getItem().get(REF_DB_VALUE_KEY).getS();

      // TODO: not every string might be cast to T (it should work now because the
      // only usage of this function requests string, but we should be serializing
      // deserializing objects before adding them to dynamo.
      return Optional.of((T) refValue);
    } catch (Exception e) {
      logger.atSevere().withCause(e).log("Cannot get value for %s", pathFor(project, refName));
      return Optional.empty();
    }
  }

  private GetItemResult getPathFromDynamoDB(Project.NameKey project, String refName) {
    return dynamoDBClient.getItem(
        configuration.getRefsDbTableName(),
        ImmutableMap.of(REF_DB_PRIMARY_KEY, new AttributeValue(pathFor(project, refName))),
        true);
  }

  private boolean exists(GetItemResult result) {
    return result.getItem() != null && !result.getItem().isEmpty();
  }
}
