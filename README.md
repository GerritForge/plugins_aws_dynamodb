# Gerrit DynamoDB ref-db

This plugin provides an implementation of the Gerrit global ref-db backed by
[AWS DynamoDB](https://aws.amazon.com/dynamodb/).

Requirements for using this plugin are:

- Gerrit v3.2 or later
- DynamoDB provisioned in AWS

## Typical use-case

The global ref-db is a typical use-case of a Gerrit multi-master scenario
in a multi-site setup. Refer to the
[Gerrit multi-site plugin](https://gerrit.googlesource.com/plugins/multi-site/+/master/DESIGN.md)
for more details on the high level architecture.
