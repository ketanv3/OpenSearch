# CHANGELOG

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Add project health badges to the README.md ([#4843](https://github.com/opensearch-project/OpenSearch/pull/4843))
- Added changes for graceful node decommission ([#4586](https://github.com/opensearch-project/OpenSearch/pull/4586))
- Build no-jdk distributions as part of release build ([#4902](https://github.com/opensearch-project/OpenSearch/pull/4902))
- Renamed flaky tests ([#4912](https://github.com/opensearch-project/OpenSearch/pull/4912))
- Update previous release bwc version to 2.5.0 ([#5003](https://github.com/opensearch-project/OpenSearch/pull/5003))
- Use getParameterCount instead of getParameterTypes ([#4821](https://github.com/opensearch-project/OpenSearch/pull/4821))
- Remote shard balancer support for searchable snapshots ([#4870](https://github.com/opensearch-project/OpenSearch/pull/4870))
- [Test] Add IAE test for deprecated edgeNGram analyzer name ([#5040](https://github.com/opensearch-project/OpenSearch/pull/5040))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add feature flag for extensions ([#5211](https://github.com/opensearch-project/OpenSearch/pull/5211))
- Added precommit support for windows ([#4676](https://github.com/opensearch-project/OpenSearch/pull/4676))
- Added release notes for 1.3.6 ([#4681](https://github.com/opensearch-project/OpenSearch/pull/4681))
- Added precommit support for MacOS ([#4682](https://github.com/opensearch-project/OpenSearch/pull/4682))
- Recommission API changes for service layer ([#4320](https://github.com/opensearch-project/OpenSearch/pull/4320))
- Update GeoGrid base class access modifier to support extensibility ([#4572](https://github.com/opensearch-project/OpenSearch/pull/4572))
- Add a new node role 'search' which is dedicated to provide search capability ([#4689](https://github.com/opensearch-project/OpenSearch/pull/4689))
- Introduce experimental searchable snapshot API ([#4680](https://github.com/opensearch-project/OpenSearch/pull/4680))
- Recommissioning of zone. REST layer support. ([#4624](https://github.com/opensearch-project/OpenSearch/pull/4604))

### Dependencies
- Bumps `log4j-core` from 2.18.0 to 2.19.0
- Bumps `reactor-netty-http` from 1.0.18 to 1.0.23
- Bumps `jettison` from 1.5.0 to 1.5.1
- Bumps `azure-storage-common` from 12.18.0 to 12.18.1
- Bumps `forbiddenapis` from 3.3 to 3.4
- Bumps `gson` from 2.9.0 to 2.10
- Bumps `protobuf-java` from 3.21.2 to 3.21.9
- Bumps `azure-core` from 1.31.0 to 1.33.0
- Bumps `avro` from 1.11.0 to 1.11.1
- Bumps `woodstox-core` from 6.3.0 to 6.3.1
- Bumps `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bumps `azure-core-http-netty` from 1.12.0 to 1.12.4 ([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps `azure-core` from 1.27.0 to 1.31.0 ([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps `azure-storage-common` from 12.16.0 to 12.18.0 ([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps `org.gradle.test-retry` from 1.4.0 to 1.4.1 ([#4411](https://github.com/opensearch-project/OpenSearch/pull/4411))
- Bumps `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bumps `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bumps `com.diffplug.spotless` from 6.10.0 to 6.11.0 ([#4547](https://github.com/opensearch-project/OpenSearch/pull/4547))
- Bumps `reactor-core` from 3.4.18 to 3.4.23 ([#4548](https://github.com/opensearch-project/OpenSearch/pull/4548))
- Bumps `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Bumps `commons-compress` from 1.21 to 1.22
- Bumps `jcodings` from 1.0.57 to 1.0.58 ([#5233](https://github.com/opensearch-project/OpenSearch/pull/5233))
- Bumps `google-http-client-jackson2` from 1.35.0 to 1.42.3 ([#5234](https://github.com/opensearch-project/OpenSearch/pull/5234))
- Bumps `maxmind-db` from 2.0.0 to 2.1.0 ([#5236](https://github.com/opensearch-project/OpenSearch/pull/5236))
- Bumps `azure-core` from 1.33.0 to 1.34.0 ([#5235](https://github.com/opensearch-project/OpenSearch/pull/5235))
- Bumps `azure-core-http-netty` from 1.12.4 to 1.12.7 ([#5235](https://github.com/opensearch-project/OpenSearch/pull/5235))
- Bumps `spock-core` from from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#5315](https://github.com/opensearch-project/OpenSearch/pull/5315))
- Bumps `json-schema-validator` from 1.0.69 to 1.0.73 ([#5316](https://github.com/opensearch-project/OpenSearch/pull/5316))
- Bumps `proto-google-common-protos` from 2.8.0 to 2.10.0 ([#5318](https://github.com/opensearch-project/OpenSearch/pull/5318))
- Bumps `protobuf-java` from 3.21.7 to 3.21.9 ([#5319](https://github.com/opensearch-project/OpenSearch/pull/5319))
- Update Apache Lucene to 9.5.0-snapshot-a4ef70f ([#4979](https://github.com/opensearch-project/OpenSearch/pull/4979))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Support remote translog transfer for request level durability([#4480](https://github.com/opensearch-project/OpenSearch/pull/4480))
- Changed http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fixed compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Reject bulk requests with invalid actions ([#5299](https://github.com/opensearch-project/OpenSearch/issues/5299))
- `opensearch-service.bat start` and `opensearch-service.bat manager` failing to run ([#4289](https://github.com/opensearch-project/OpenSearch/pull/4289))
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- `opensearch.bat` and `opensearch-service.bat install` failing to run, missing logs directory ([#4305](https://github.com/opensearch-project/OpenSearch/pull/4305))
- Restore using the class ClusterInfoRequest and ClusterInfoRequestBuilder from package 'org.opensearch.action.support.master.info' for subclasses ([#4307](https://github.com/opensearch-project/OpenSearch/pull/4307))
- Do not fail replica shard due to primary closure ([#4133](https://github.com/opensearch-project/OpenSearch/pull/4133))
- Add timeout on Mockito.verify to reduce flakyness in testReplicationOnDone test([#4314](https://github.com/opensearch-project/OpenSearch/pull/4314))
- Commit workflow for dependabot changelog helper ([#4331](https://github.com/opensearch-project/OpenSearch/pull/4331))
- Fixed cancellation of segment replication events ([#4225](https://github.com/opensearch-project/OpenSearch/pull/4225))
- [Segment Replication] Bump segment infos counter before commit during replica promotion ([#4365](https://github.com/opensearch-project/OpenSearch/pull/4365))
- Bugs for dependabot changelog verifier workflow ([#4364](https://github.com/opensearch-project/OpenSearch/pull/4364))
- Fix flaky random test `NRTReplicationEngineTests.testUpdateSegments` ([#4352](https://github.com/opensearch-project/OpenSearch/pull/4352))
- [Segment Replication] Extend FileChunkWriter to allow cancel on transport client ([#4386](https://github.com/opensearch-project/OpenSearch/pull/4386))
- [Segment Replication] Add check to cancel ongoing replication with old primary on onNewCheckpoint on replica ([#4363](https://github.com/opensearch-project/OpenSearch/pull/4363))
- Fix NoSuchFileExceptions with segment replication when computing primary metadata snapshots ([#4366](https://github.com/opensearch-project/OpenSearch/pull/4366))
- [Segment Replication] Update flaky testOnNewCheckpointFromNewPrimaryCancelOngoingReplication unit test ([#4414](https://github.com/opensearch-project/OpenSearch/pull/4414))
- Fixed the `_cat/shards/10_basic.yml` test cases fix.
- [Segment Replication] Fix timeout issue by calculating time needed to process getSegmentFiles ([#4426](https://github.com/opensearch-project/OpenSearch/pull/4426))
- [Bug]: gradle check failing with java heap OutOfMemoryError (([#4328](https://github.com/opensearch-project/OpenSearch/
- `opensearch.bat` fails to execute when install path includes spaces ([#4362](https://github.com/opensearch-project/OpenSearch/pull/4362))

### Security

## [Unreleased 2.x]
### Added
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5069](https://github.com/opensearch-project/OpenSearch/pull/5069))

### Dependencies
- Bumps `bcpg-fips` from 1.0.5.1 to 1.0.7.1
- Bumps `azure-storage-blob` from 12.16.1 to 12.20.0 ([#4995](https://github.com/opensearch-project/OpenSearch/pull/4995))
- Bumps `commons-compress` from 1.21 to 1.22 ([#5104](https://github.com/opensearch-project/OpenSearch/pull/5104))
- Bump `opencensus-contrib-http-util` from 0.18.0 to 0.31.1 ([#3633](https://github.com/opensearch-project/OpenSearch/pull/3633))
- Bump `geoip2` from 3.0.1 to 3.0.2 ([#5103](https://github.com/opensearch-project/OpenSearch/pull/5103))
- Bump gradle-extra-configurations-plugin from 7.0.0 to 8.0.0 ([#4808](https://github.com/opensearch-project/OpenSearch/pull/4808))
### Changed
### Deprecated
### Removed
### Fixed
### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.4...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.4...2.x
