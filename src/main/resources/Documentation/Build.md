# Build

The plugins_aws-dynamodb plugin can be build as a regular 'in-tree' plugin. That means
that is required to clone a Gerrit source tree first and then to have the plugin
source directory into the `/plugins` path.

Additionally, the `plugins/external_plugin_deps.bzl` file needs to be updated to
match the plugins_aws-dynamodb plugin one.

```shell script
git clone --recursive https://gerrit.googlesource.com/gerrit
cd gerrit
git clone "https://review.gerrithub.io/GerritForge/plugins_aws-dynamodb" plugins/plugins_aws-dynamodb
ln -sf plugins/plugins_aws-dynamodb/external_plugin_deps.bzl plugins/.
bazelisk build plugins/plugins_aws-dynamodb
```

The output is created in

```
bazel-genfiles/plugins/plugins_aws-dynamodb/plugins_aws-dynamodb.jar
```