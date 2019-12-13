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

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.datastore.folder.FolderDataStore;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.datastore.folder.FolderDataStoreManager;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Properties;

class KafkaOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport {

  private static final BundleLogger LOGGER	= BundleLoggerFactory.getLogger(KafkaOutboundTransport.class);
  private static final String CLIENT_ID = "kafka-for-geoevent";
  private final FolderDataStoreManager folderDataStoreManager;

  private String bootstrapServers = "";
  private String topic = "";
  private String advancedConfigs = "";

  private String partitionKeyTag = "";

  private KafkaEventProducer producer;
  private Properties advancedConfigProperties;

  KafkaOutboundTransport(TransportDefinition definition, FolderDataStoreManager folderDataStoreManager) throws ComponentException {
    super(definition);
    this.folderDataStoreManager = folderDataStoreManager;
  }

  @Override
  public synchronized void receive(final ByteBuffer byteBuffer, String channelId) {
    receive(byteBuffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer byteBuffer, String channelId, GeoEvent geoEvent) {
    try {
      if (geoEvent != null) {
        Object partitionKey = null;

        if(partitionKeyTag != null && !partitionKeyTag.isEmpty())
        {
          final int tagIndex = geoEvent.getGeoEventDefinition().getIndexOf(partitionKeyTag);

          if (tagIndex >= 0) {
            partitionKey = geoEvent.getField(tagIndex);
          }
          else
          {
            final String warnMsg = LOGGER.translate("NO_MATCHING_TAG_WARNING",
                    geoEvent.getGeoEventDefinition()
                            .getName(),
                    partitionKeyTag);
            LOGGER.warn(warnMsg);
          }
        }

        producer.send(byteBuffer, partitionKey);
      }
    }
    catch (MessagingException e)
    {
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug(e.getMessage(), e.getCause());
      }
    }
  }

  @SuppressWarnings("incomplete-switch")
  public synchronized void start() throws RunningException {
    switch (getRunningState())
    {
      case STOPPING:
      case STOPPED:
      case ERROR:
        connect();
        break;
    }
  }

  @Override
  public synchronized void stop() {
    if (!RunningState.STOPPED.equals(getRunningState()))
      disconnect("");
  }

  @Override
  public void afterPropertiesSet() {

    bootstrapServers = getProperty(KafkaTransportDefinitionBase.PROPERTY_BOOTSTRAP_SERVER).getValueAsString();
    topic = getProperty(KafkaTransportDefinitionBase.PROPERTY_TOPIC).getValueAsString();
    advancedConfigs = getProperty(KafkaTransportDefinitionBase.PROPERTY_ADVANCED_CONFIGS).getValueAsString();

    final String advancedConfigsFolder = getProperty(KafkaTransportDefinitionBase.PROPERTY_ADVANCED_CONFIGS_FOLDER).getValueAsString();
    final String advancedConfigsFileName = getProperty(KafkaTransportDefinitionBase.PROPERTY_ADVANCED_CONFIGS_FILE_NAME).getValueAsString();

    if(!advancedConfigsFolder.isEmpty() &&
        !advancedConfigsFileName.isEmpty()) {
      FolderDataStore folderDataStore = folderDataStoreManager.getFolderDataStore(advancedConfigsFolder);
      try (final InputStream inputStream =
               new FileInputStream(new File(folderDataStore.getPath(), advancedConfigsFileName)))
      {
        final Properties properties = new Properties();
        properties.load(inputStream);
        this.advancedConfigProperties = properties;
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
      }
    }
    partitionKeyTag = getProperty("partitionKeyTag").getValueAsString();

    super.afterPropertiesSet();
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (bootstrapServers.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    if (topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    // TODO: Validate advanced configs
  }

  private synchronized void disconnect(String reason) {
    setRunningState(RunningState.STOPPING);
    if (producer != null) {
      producer.disconnect();
      producer = null;
    }
    setErrorMessage(reason);
    setRunningState(RunningState.STOPPED);
  }

  private synchronized void connect() {
    disconnect("");
    setRunningState(RunningState.STARTING);
    try {
      producer = new KafkaEventProducer(new EventDestination(topic), new Properties(advancedConfigProperties), bootstrapServers);
      producer.setup();
      setRunningState(RunningState.STARTED);
    }
    catch(Exception ex) {
      setRunningState(RunningState.ERROR);
      setErrorMessage(ex.getMessage());
      LOGGER.error(ex.getMessage());
    }
  }

  private synchronized void shutdownProducer() {
    if (producer != null) {
      producer.shutdown();
      producer = null;
    }
  }

  public void shutdown() {
    shutdownProducer();
    super.shutdown();
  }

  private class KafkaEventProducer extends KafkaComponentBase {
    private KafkaProducer<String, ByteBuffer> producer;

    private final Callback completionCallback = (metadata, e) -> {
      if (e != null)
      {
        final String errorMsg = LOGGER.translate("KAFKA_SEND_FAILURE_ERROR",
            metadata.topic(),
            metadata.partition(),
            metadata.offset(),
            e.getMessage());

        LOGGER.error(errorMsg);
        // offset = metadata.offset();
        return;
      }

      if(LOGGER.isDebugEnabled())
      {
        final String debugMsg = LOGGER.translate("KAFKA_SENT_RECORD_DEBUG",
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.serializedKeySize(),
                metadata.serializedValueSize());
        LOGGER.debug(debugMsg);
      }
    };

    KafkaEventProducer(EventDestination destination, Properties properties, String bootstrap) {
      super(destination, properties);

      // http://kafka.apache.org/documentation.html#producerconfigs
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);

      try {
        if(advancedConfigs != null && !advancedConfigs.isEmpty()) {
          props.load(new StringReader(advancedConfigs));
        }
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
        e.printStackTrace();
      }

      // These two properties must override any custom properties
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteBufferSerializer.class.getName());

      LOGGER.info(props.toString());
    }

    @Override
    public synchronized void init() throws MessagingException {
      if (producer == null) {
        try {
          Thread.currentThread().setContextClassLoader(null); // see http://stackoverflow.com/questions/34734907/karaf-kafka-osgi-bundle-producer-issue for details
          producer = new KafkaProducer<>(props);
        }
        catch(Exception ex) {
          throw new MessagingException(ex.getMessage());
        }
      }
    }

    @Override
    protected void setConnected() {
      super.setConnected();
      setRunningState(RunningState.STARTED);
    }

    public void send(final ByteBuffer bb, Object partitionKey) throws MessagingException {
      try {
        // wait to send messages if we are not connected
        if (isConnected()) {
          final ProducerRecord<String, ByteBuffer> record;

          if (partitionKey != null) {
            record = new ProducerRecord<>(
                destination.getName(),
                partitionKey.toString(),
                bb);
          } else {
            record = new ProducerRecord<>(
                destination.getName(),
                bb);
          }

          producer.send(record, completionCallback);
        }
      } catch(Exception ex) {
        LOGGER.error(ex.getMessage());
      }
    }

    @Override
    public synchronized void disconnect() {
      if (producer != null) {
        producer.close();
        producer = null;
      }
      super.disconnect();
    }

    @Override
    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }
  }
}
