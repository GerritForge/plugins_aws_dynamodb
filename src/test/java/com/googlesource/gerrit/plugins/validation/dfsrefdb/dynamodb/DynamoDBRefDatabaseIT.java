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
import static com.google.common.truth.Truth8.assertThat;
import static com.google.gerrit.testing.GerritJUnit.assertThrows;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.Configuration.DEFAULT_LOCKS_TABLE_NAME;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.Configuration.DEFAULT_REFS_DB_TABLE_NAME;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.DynamoDBRefDatabase.REF_DB_PRIMARY_KEY;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.DynamoDBRefDatabase.REF_DB_VALUE_KEY;
import static com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.DynamoDBRefDatabase.pathFor;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.gerritforge.gerrit.globalrefdb.GlobalRefDbSystemError;
import com.google.common.collect.ImmutableMap;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.WaitUtil;
import com.google.gerrit.common.Nullable;
import com.google.gerrit.entities.Project;
import java.time.Duration;
import java.util.Optional;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

@TestPlugin(
    name = "plugins_aws-dynamodb",
    sysModule = "com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.Module")
public class DynamoDBRefDatabaseIT extends LightweightPluginDaemonTest {
  private static final Duration DYNAMODB_TABLE_CREATION_TIMEOUT = Duration.ofSeconds(10);

  private static final int LOCALSTACK_PORT = 4566;
  private static final LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.12.8"))
          .withServices(DYNAMODB)
          .withExposedPorts(LOCALSTACK_PORT);

  @Before
  public void setUpTestPlugin() throws Exception {
    localstack.start();

    System.setProperty("endpoint", localstack.getEndpointOverride(DYNAMODB).toASCIIString());
    System.setProperty("region", localstack.getRegion());
    System.setProperty("aws.accessKeyId", localstack.getAccessKey());

    // The secret key property name has changed from aws-sdk 1.11.x and 2.x [1]
    // Export both names so that default credential provider chains work regardless
    // he underlying library version.
    // https: // docs.aws.amazon.com/sdk-for-java/latest/migration-guide/client-credential.html
    System.setProperty("aws.secretKey", localstack.getSecretKey());
    System.setProperty("aws.secretAccessKey", localstack.getSecretKey());

    super.setUpTestPlugin();
  }

  @Override
  public void tearDownTestPlugin() {
    localstack.close();

    super.tearDownTestPlugin();
  }

  @Test
  public void shouldEnsureLockTableExists() throws Exception {
    WaitUtil.waitUntil(
        () -> DynamoDBLifeCycleManager.tableExists(dynamoDBClient(), DEFAULT_LOCKS_TABLE_NAME),
        DYNAMODB_TABLE_CREATION_TIMEOUT);
  }

  @Test
  public void shouldEnsureRefsDbTableExists() throws Exception {
    WaitUtil.waitUntil(
        () -> DynamoDBLifeCycleManager.tableExists(dynamoDBClient(), DEFAULT_REFS_DB_TABLE_NAME),
        DYNAMODB_TABLE_CREATION_TIMEOUT);
  }

  @Test
  public void getShouldBeEmptyWhenRefDoesntExists() throws Exception {
    Optional<String> maybeRef = dynamoDBRefDatabase().get(project, "refs/not/in/db", String.class);

    assertThat(maybeRef).isEmpty();
  }

  @Test
  public void getShouldReturnRefValueWhenItExists() throws Exception {
    String refName = "refs/changes/01/01/meta";
    String refValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";
    createRefInDynamoDB(project, refName, refValue);

    Optional<String> maybeRef = dynamoDBRefDatabase().get(project, refName, String.class);

    assertThat(maybeRef).hasValue(refValue);
  }

  @Test
  public void existsShouldReturnFalseWhenRefIsNotStored() throws Exception {
    assertThat(dynamoDBRefDatabase().exists(project, "refs/not/in/db")).isFalse();
  }

  @Test
  public void existShouldReturnTrueWhenRefIsStored() throws Exception {
    String refName = "refs/changes/01/01/meta";
    String refValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";
    createRefInDynamoDB(project, refName, refValue);

    assertThat(dynamoDBRefDatabase().exists(project, refName)).isTrue();
  }

  @Test
  public void isUpToDateShouldReturnTrueWhenRefPointsToTheStoredRefValue() throws Exception {
    String refName = "refs/changes/01/01/meta";
    String currentRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";

    createRefInDynamoDB(project, refName, currentRefValue);

    assertThat(dynamoDBRefDatabase().isUpToDate(project, refOf(refName, currentRefValue))).isTrue();
  }

  @Test
  public void isUpToDateShouldReturnFalseWhenRefDoesNotPointToTheStoredRefValue() {
    String refName = "refs/changes/01/01/meta";
    String currentRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";
    String previousRefValue = "9f6f2963cf44505428c61b935ff1ca65372cf28c";

    createRefInDynamoDB(project, refName, previousRefValue);

    assertThat(dynamoDBRefDatabase().isUpToDate(project, refOf(refName, currentRefValue)))
        .isFalse();
  }

  @Test
  public void isUpToDateShouldBeConsideredTrueWhenNoPreviousRefExists() {
    String refName = "refs/changes/01/01/meta";
    String currentRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";

    assertThat(dynamoDBRefDatabase().isUpToDate(project, refOf(refName, currentRefValue))).isTrue();
  }

  @Test
  public void compareAndPutShouldBeSuccessfulWhenNoPreviousRefExists() {
    String refName = "refs/changes/01/01/meta";
    String newRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";

    assertThat(
            dynamoDBRefDatabase()
                .compareAndPut(project, refOf(refName, null), ObjectId.fromString(newRefValue)))
        .isTrue();
  }

  @Test
  public void compareAndPutShouldSuccessfullyUpdateRemovedRef() throws Exception {
    String refName = "refs/changes/01/01/meta";
    String currentRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";

    createRefInDynamoDB(project, refName, currentRefValue);

    assertThat(
            dynamoDBRefDatabase()
                .compareAndPut(project, refOf(refName, currentRefValue), ObjectId.zeroId()))
        .isTrue();
  }

  @Test
  public void compareAndPutShouldThrowWhenStoredRefIsNotExpected() {
    String refName = "refs/changes/01/01/meta";
    String currentRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";
    String newRefValue = "9f6f2963cf44505428c61b935ff1ca65372cf28c";
    String expectedRefValue = "875ce4b14278b64be61478f91a40cf480758bfba";
    Ref expectedRef = refOf(refName, expectedRefValue);

    createRefInDynamoDB(project, refName, currentRefValue);

    GlobalRefDbSystemError thrown =
        assertThrows(
            GlobalRefDbSystemError.class,
            () ->
                dynamoDBRefDatabase()
                    .compareAndPut(project, expectedRef, ObjectId.fromString(newRefValue)));
    assertThat(thrown)
        .hasMessageThat()
        .containsMatch(
            "Conditional Check Failure when updating refPath.*expected:.*"
                + expectedRefValue
                + " New:.*"
                + newRefValue);
  }

  @Test
  public void compareAndPutStringsShouldBeSuccessful() throws Exception {
    String refName = "refs/changes/01/01/meta";
    String currentRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";
    String newRefValue = "9f6f2963cf44505428c61b935ff1ca65372cf28c";

    createRefInDynamoDB(project, refName, currentRefValue);

    assertThat(dynamoDBRefDatabase().compareAndPut(project, refName, currentRefValue, newRefValue))
        .isTrue();
  }

  @Test
  public void compareAndPutStringsShouldBeSuccessfulWhenNoPreviousRefExists() {
    String refName = "refs/changes/01/01/meta";
    String newRefValue = "533d3ccf8a650fb26380faa732921a2c74924d5c";

    assertThat(dynamoDBRefDatabase().compareAndPut(project, refName, null, newRefValue)).isTrue();
  }

  private AmazonDynamoDB dynamoDBClient() {
    return plugin.getSysInjector().getInstance(AmazonDynamoDB.class);
  }

  private DynamoDBRefDatabase dynamoDBRefDatabase() {
    return plugin.getSysInjector().getInstance(DynamoDBRefDatabase.class);
  }

  private void createRefInDynamoDB(Project.NameKey project, String refPath, String refValue) {
    dynamoDBClient()
        .putItem(
            new PutItemRequest()
                .withTableName(DEFAULT_REFS_DB_TABLE_NAME)
                .withItem(
                    ImmutableMap.of(
                        REF_DB_PRIMARY_KEY,
                        new AttributeValue(pathFor(project, refPath)),
                        REF_DB_VALUE_KEY,
                        new AttributeValue(refValue))));
  }

  private Ref refOf(String refName, @Nullable String objectIdSha1) {
    return new ObjectIdRef.Unpeeled(
        Ref.Storage.NETWORK,
        refName,
        Optional.ofNullable(objectIdSha1).map(ObjectId::fromString).orElse(null));
  }
}
