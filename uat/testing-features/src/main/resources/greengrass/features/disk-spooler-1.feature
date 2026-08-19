@DiskSpooler
Feature: DiskSpooler-1

  As a customer, I can spool outbound MQTT messages so that they persist across Nucleus restarts.

  Background:
    Given my device is registered as a Thing
    And my device is running Greengrass
    And I start an assertion server

  Scenario: DiskSpooler-1-T1: MQTT messages are published to IoT Core
    Then I create a random name as cloud_topic
    When I subscribe to cloud topics
      | ${cloud_topic} |
    Given I create a Greengrass deployment with components
      | aws.greengrass.Cli         | LATEST                                    |
      | aws.greengrass.DiskSpooler | classpath:/greengrass/recipes/recipe.yaml |
    And I update my Greengrass deployment configuration, setting the component aws.greengrass.Nucleus configuration to:
      """
      {
        "MERGE": {
          "mqtt": {
            "spooler": {
              "storageType": "Disk"
            }
          },
          "logging": {
            "level": "DEBUG"
          }
        }
      }
      """
    And I deploy the Greengrass deployment configuration
    Then the Greengrass deployment is COMPLETED on the device after 3 minutes
    Then I verify the aws.greengrass.DiskSpooler component is RUNNING using the greengrass-cli
    Then I wait 20 seconds
    When I install the component IotMqttPublisher from local store with configuration
      """
      {
        "MERGE": {
          "assertionServerPort": ${assertionServerPort},
          "topic": "${cloud_topic}",
          "payload": "Hello world",
          "qos": "1",
          "accessControl": {
            "aws.greengrass.ipc.mqttproxy": {
              "policyId1": {
                "policyDescription": "access to publish to mqtt topics",
                "operations": [
                  "aws.greengrass#PublishToIoTCore"
                ],
                "resources": [
                  "${cloud_topic}"
                ]
              }
            }
          }
        }
      }
      """
    And I get 1 assertion with context "Successfully published to IoT topic ${cloud_topic}"
    Then the cloud topic ${cloud_topic} receives the following messages within 10 seconds
      | Hello world |


#  TODO convert remaining UATs
