/*
  Copyright 1995-2016 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportType;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaInboundTransportDefinition extends KafkaTransportDefinitionBase {
  private static final BundleLogger	LOGGER	= BundleLoggerFactory.getLogger(KafkaInboundTransportDefinition.class);

  KafkaInboundTransportDefinition()
  {
    super(TransportType.INBOUND);
    try
    {
      propertyDefinitions.put("numThreads", new PropertyDefinition("numThreads",
          PropertyType.Integer,
          "1",
          "${com.esri.geoevent.transport.kafka-advanced-transport.NUM_THREADS_LBL}",
          "${com.esri.geoevent.transport.kafka-advanced-transport.NUM_THREADS_DESC}",
          true,
          false));

      propertyDefinitions.put("groupId", new PropertyDefinition("groupId",
          PropertyType.String,
          "",
          "${com.esri.geoevent.transport.kafka-advanced-transport.GROUP_ID_LBL}",
          "${com.esri.geoevent.transport.kafka-advanced-transport.GROUP_ID_DESC}",
          false,
          false));
    }
    catch (PropertyException e)
    {
      String errorMsg = LOGGER.translate("TRANSPORT_IN_INIT_ERROR", e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.transport.inbound";
  }

  @Override
  public String getLabel()
  {
    return "${com.esri.geoevent.transport.kafka-advanced-transport.IN_LABEL}";
  }

  @Override
  public String getDescription()
  {
    return "${com.esri.geoevent.transport.kafka-advanced-transport.IN_DESC}";
  }
}
