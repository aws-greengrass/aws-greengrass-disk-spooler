#!/usr/bin/env bash
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
set -e
curl -s https://d2s8p88vqu9w66.cloudfront.net/releases/greengrass-nucleus-latest.zip > greengrass-nucleus-latest.zip
mvn -U -ntp clean package -DskipTests
mvn -U -ntp clean verify -f uat/pom.xml
sudo java -Dggc.archive=greengrass-nucleus-latest.zip -Dtest.log.path=uat-results -Dtags=DiskSpooler -jar uat/testing-features/target/greengrass-disk-spooler-testing-features.jar
