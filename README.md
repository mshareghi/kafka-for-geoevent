# kafka-for-geoevent

ArcGIS GeoEvent Server sample Kafka connectors for connecting to Kafka message servers to send and/or receive text messages.

![App](kafka-for-geoevent.png?raw=true)

## Changes in this Fork

This branch provides additional capabilities:
- Upgrade to the new Kafka consumer API and kafka-clients 2.3.1. A Zookeeper connection is no longer required.
- You can specify custom Kafka configuration parameters like `compression.type`, `linger.ms` and TLS settings. 
- The names and versions have been changed to reflect a difference from the upstream code.

*This code is provided under the Apache 2.0 license and is not officially maintained or supported by the Esri GeoEvent team.*

## Features
* Kafka Advanced Inbound Transport
* Kafka Advanced Outbound Transport

## Instructions

Building the source code:

1. Make sure Maven and ArcGIS GeoEvent Server SDK are installed on your machine.
2. Run 'mvn install -Dcontact.address=[YourContactEmailAddress]'

Installing the built jar files:

1. Copy the *.jar files under the 'target' sub-folder(s) into the [ArcGIS-GeoEvent-Server-Install-Directory]/deploy folder.

## Requirements

* ArcGIS GeoEvent Server.
* ArcGIS GeoEvent Server SDK.
* Java JDK 1.8 or greater.
* Maven.

## Resources

* [Download the connector's tutorial](http://www.arcgis.com/home/item.html?id=7f94ec2a3eb944c79e98fe854d60d671) from the ArcGIS GeoEvent Gallery
* [ArcGIS GeoEvent Server resources](http://links.esri.com/geoevent)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing
Copyright 2019 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt](license.txt?raw=true) file.
