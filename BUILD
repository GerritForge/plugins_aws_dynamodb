load("//tools/bzl:junit.bzl", "junit_tests")
load(
    "//tools/bzl:plugin.bzl",
    "PLUGIN_DEPS",
    "PLUGIN_TEST_DEPS",
    "gerrit_plugin",
)

gerrit_plugin(
    name = "plugins_aws-dynamodb",
    srcs = glob(["src/main/java/**/*.java"]),
    manifest_entries = [
        "Gerrit-PluginName: plugins_aws-dynamodb",
        "Gerrit-Module: com.googlesource.gerrit.plugins.validation.dfsrefdb.dynamodb.Module",
        "Implementation-Title: dynamodb ref-db plugin",
        "Implementation-URL: https://review.gerrithub.io/admin/repos/GerritForge/plugins_aws-dynamodb",
    ],
    resources = glob(["src/main/resources/**/*"]),
    deps = [
        "@global-refdb//jar",
        "@amazon-dynamodb//jar",
        "@aws-java-sdk-dynamodb//jar",
        "@dynamodb-lock-client//jar",
        "@amazon-regions//jar",
        "@amazon-sdk-core//jar",
        "@amazon-aws-core//jar",
        "@aws-java-sdk-core//jar",
        "@amazon-utils//jar",
        "@jackson-databind//jar",
        "@jackson-dataformat-cbor//jar",
        "@jackson-annotations//jar",
        "@joda-time//jar",
    ],
)

junit_tests(
    name = "plugins_aws-dynamodb_tests",
    srcs = glob(["src/test/java/**/*.java"]),
    resources = glob(["src/test/resources/**/*"]),
    tags = ["plugins_aws-dynamodb"],
    deps = [
        ":plugins_aws-dynamodb__plugin_test_deps",
    ],
)

java_library(
    name = "plugins_aws-dynamodb__plugin_test_deps",
    testonly = 1,
    visibility = ["//visibility:public"],
    exports = PLUGIN_DEPS + PLUGIN_TEST_DEPS + [
        ":plugins_aws-dynamodb__plugin",
    ],
)
