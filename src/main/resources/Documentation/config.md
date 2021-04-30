Configuration
=========================

The dynamodb-refdb plugin is configured by adding a plugin stanza in the
`gerrit.config` file, for example:

```text
[plugin "plugins_aws-dynamodb"]
    region = us-east-1
    endpoint = http://localhost:4566
    locksTableName = lockTable
    refsDbTableName = refsDb
```

`plugin.plugins_aws-dynamodb.region`
:   Optional. Which AWS region to connect to.
Default: When not specified this value is provided via the default Region
Provider Chain, as explained [here](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)

`plugin.plugins_aws-dynamodb.endpoint`
:   Optional. When defined, it will override the default dynamodb endpoint
will connect to it, rather than connecting to AWS. This is useful when
developing or testing, in order to connect locally.
See [localstack](https://github.com/localstack/localstack) to understand
more about how run dynamodb stack outside AWS.
Default: <empty>

`plugin.plugins_aws-dynamodb.locksTableName`
:   Optional. The name of the dynamoDB table used to store distribute locking
See [DynamoDB lock client](https://github.com/awslabs/amazon-dynamodb-lock-client)

`plugin.plugins_aws-dynamodb.locksTableName`
:   Optional. The name of the dynamoDB table used to store git refs and their
associated sha1.


