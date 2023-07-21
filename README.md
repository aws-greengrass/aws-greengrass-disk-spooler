# AWS IoT Greengrass Disk Spooler Component

### *Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.*

#### *SPDX-License-Identifier: Apache-2.0*
<br>

<img src="https://img.shields.io/badge/OS-linux%20%7C%20windows-blue??style=flat&logo=Linux&logoColor=b0c0c0&labelColor=363D44" alt="Operating systems"/>


<br>

The AWS IoT Greengrass Disk Spooler Component (aws.greengrass.DiskSpooler) offers persistent storage option when Greengrass device is offline. Messages destined to AWS IoT Core are queued in-memory on the device by default. Deploy this plugin component and configure the [Greengrass Nucleus component](https://github.com/aws-greengrass/aws-greengrass-nucleus) to persist messages across device power cycles. 

## Requirements

AWS IoT Greengrass Nucleus component version `2.11.0` or greater must be installed. 

## Installation
Deploy this component and customize the following configuration parameters of the Greengrass Nucleus component:

```
    mqtt:
        spooler:
          storageType: Disk
          pluginName: "aws.greengrass.DiskSpooler"
```

## More Resources

- Please visit [Disk Spooler](https://docs.aws.amazon.com/greengrass/v2/developerguide/disk-spooler-component.html) for full documentation of the component. 
- Interested in contributing to this project? Please see [Contributing](CONTRIBUTING.md).
- Need to report a security issue? Please see [Security](CONTRIBUTING.md#security-issue-notifications).

