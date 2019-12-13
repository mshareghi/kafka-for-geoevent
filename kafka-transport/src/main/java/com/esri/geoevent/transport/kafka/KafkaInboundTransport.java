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
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.datastore.folder.FolderDataStore;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.datastore.folder.FolderDataStoreManager;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.WakeupException;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class KafkaInboundTransport extends InboundTransportBase {
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(KafkaInboundTransport.class);
  private static final String CLIENT_ID = "kafka-for-geoevent";
  private static final Duration CONSUMER_MAX_POLL_TIME = Duration.ofMillis(Long.MAX_VALUE);
  private final FolderDataStoreManager folderDataStoreManager;

  private ExecutorService executorService;

  private String bootstrapServers = "";
  private String topic = "";
  private String advancedConfigs = "";

  private String groupId = "";
  private int numThreads = 1;

  private List<KafkaConsumerLoop> consumers = new ArrayList<>();
  private Properties advancedConfigProperties;

  KafkaInboundTransport(TransportDefinition definition, FolderDataStoreManager folderDataStoreManager) throws ComponentException {
    super(definition);
    this.folderDataStoreManager = folderDataStoreManager;
  }

  public boolean isClusterable() {
    return true;
  }

  private class KafkaConsumerLoop implements Runnable
  {
    private final KafkaConsumer<String, ByteBuffer> consumer;

    private final List<String> topics;
    private final String clientId;
    private final int id;

    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final ConsumerRebalanceListener listener;

    KafkaConsumerLoop(int id, List<String> topics, Properties configs) {
      this.id = id;
      this.consumer = new KafkaConsumer<>(configs);
      this.clientId = configs.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
      this.topics = topics;

      this.listener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          LOGGER.info(String.format("Consumer [ %s | %d ]- Revoked partitions : %s", clientId, id, partitions));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          LOGGER.info(String.format("Consumer [ %s | %d ] - Assigned partitions : %s", clientId, id, partitions));
        }
      };
    }

    @Override
    public void run()
    {
      try
      {
        LOGGER.info(String.format("Starting Consumer [ %s | %d ]", clientId, id));

        this.consumer.subscribe(topics, listener);

        if(LOGGER.isDebugEnabled())
        {
          LOGGER.debug(String.format("Consumer [ %s | %d ] - Subscribed to topic(s): %s", clientId, id, topics));
        }

        while (isRunning())
        {
          final ConsumerRecords<String, ByteBuffer> records = consumer.poll(CONSUMER_MAX_POLL_TIME);

          if(records.isEmpty()) {
            continue;
          }

          if(LOGGER.isDebugEnabled())
          {
            LOGGER.debug(String.format("Consumer [ %s | %d ] - Processing a batch of %d records.",
                clientId,
                id,
                records.count()));
          }

          for (ConsumerRecord<String, ByteBuffer> record : records)
          {
            final String key = record.key();
            final String partitionKey = (key == null) ? null : String.valueOf(record.partition());
            byteListener.receive(record.value(), partitionKey);
          }

          if(LOGGER.isDebugEnabled())
          {
            LOGGER.debug(String.format("Consumer [ %s | %d ] - Finished processing batch of %d records.",
                clientId,
                id,
                records.count()));
          }
        }

      }
      catch(WakeupException ex) {
        if(LOGGER.isTraceEnabled()) {
          LOGGER.trace(ex.getMessage());
        }
        // Ignore
      }
      catch(Exception ex) {
        LOGGER.error(ex.getMessage());
      }
      finally{
        LOGGER.info(String.format("Shutting down Consumer [ %s ] - Thread %d", clientId, id));
        consumer.close();
        shutdownLatch.countDown();
        LOGGER.info(String.format("Shut down Consumer [ %s ] - Thread %d", clientId, id));
      }
    }

    void shutdown(){
      try {
        consumer.wakeup();
        shutdownLatch.await();
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage());
      }
    }
  }

  @Override
  public void afterPropertiesSet() {
    bootstrapServers = getProperty(KafkaTransportDefinitionBase.PROPERTY_BOOTSTRAP_SERVER).getValueAsString();
    topic = getProperty(KafkaTransportDefinitionBase.PROPERTY_TOPIC).getValueAsString();
    advancedConfigs = getProperty(KafkaTransportDefinitionBase.PROPERTY_ADVANCED_CONFIGS).getValueAsString();
    groupId = getProperty("groupId").getValueAsString();
    if(groupId.isEmpty()) {
      try {
        groupId = String.valueOf(UUID.randomUUID());
        setProperty("groupId", groupId);
      } catch (PropertyException e) {
        LOGGER.error(e.getMessage());
      }
    }
    numThreads = Converter.convertToInteger(getProperty("numThreads").getValueAsString(), 1);

    final String advancedConfigsFolder = getProperty(KafkaTransportDefinitionBase.PROPERTY_ADVANCED_CONFIGS_FOLDER).getValueAsString();
    final String advancedConfigsFileName = getProperty(KafkaTransportDefinitionBase.PROPERTY_ADVANCED_CONFIGS_FILE_NAME).getValueAsString();

    final Properties properties = new Properties();
    this.advancedConfigProperties = properties;

    if(!advancedConfigsFolder.isEmpty() &&
        !advancedConfigsFileName.isEmpty()) {
      FolderDataStore folderDataStore = folderDataStoreManager.getFolderDataStore(advancedConfigsFolder);
      try (final InputStream inputStream =
               new FileInputStream(new File(folderDataStore.getPath(), advancedConfigsFileName)))
      {
        properties.load(inputStream);
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
      }
    }

    if(!advancedConfigs.isEmpty()) {
      try {
        properties.load(new StringReader(advancedConfigs));
      } catch(IOException ioe) {
        LOGGER.warn(ioe.getMessage());
      }
    }

    super.afterPropertiesSet();
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    if (bootstrapServers == null || bootstrapServers.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));

    try {
      ClientUtils.parseAndValidateAddresses(
          Arrays.asList(bootstrapServers.split(",")),
          ClientDnsLookup.DEFAULT);
    }
    catch (ConfigException ex) {
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    }

    if (topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));

    if (groupId.isEmpty())
      throw new ValidationException(LOGGER.translate("GROUP_ID_VALIDATE_ERROR"));

    if (numThreads < 1)
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));

    try {
      new Properties().load(new StringReader(advancedConfigs));
    } catch (IOException e) {
      throw new ValidationException("Advanced configuration is invalid.");
    }

  }

  @Override
  public synchronized void start() throws RunningException {
    switch (getRunningState()) {
      case STOPPING:
      case STOPPED:
      case ERROR:
        connect();
        break;
    }
  }

  @Override
  public synchronized void stop()
  {
    setRunningState(RunningState.STOPPING);
    disconnect("");
    setRunningState(RunningState.STOPPED);
  }

  private synchronized void disconnect(String reason)
  {
    if (!RunningState.STOPPED.equals(getRunningState()))
    {
      setRunningState(RunningState.STOPPING);
      try {
        shutdownConsumers();
        setRunningState(RunningState.STOPPED);
      }
      catch(Exception ex) {
        LOGGER.error(ex.getMessage());
        setRunningState(RunningState.ERROR);
        setErrorMessage(ex.getMessage());
      }
    }
  }

  private synchronized void connect()
  {
    disconnect("");
    setRunningState(RunningState.STARTING);

    // see http://stackoverflow.com/questions/34734907/karaf-kafka-osgi-bundle-producer-issue for details
    Thread.currentThread().setContextClassLoader(null);
    try {
      final Properties props = new Properties(advancedConfigProperties);

      props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

      try {
        props.load(new StringReader(advancedConfigs));
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
      }

      // These two properties must override any custom properties
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteBufferDeserializer.class.getName());

      final List<String> topicList = Arrays.asList(this.topic.split(",")); // account for multiple topics

      executorService = Executors.newFixedThreadPool(numThreads);

      LOGGER.info(props.toString());

      for (int i = 0; i < numThreads; i++) {
        final KafkaConsumerLoop consumer = new KafkaConsumerLoop(i, topicList, props);
        consumers.add(consumer);
        executorService.submit(consumer);
      }

      setRunningState(RunningState.STARTED);
    }
    catch (Exception ex){
      setRunningState(RunningState.ERROR);
      setErrorMessage(ex.getMessage());
    }
  }

  private void shutdownConsumers() throws InterruptedException {
    for(KafkaConsumerLoop consumer : consumers) {
      consumer.shutdown();
    }
  }

  public void shutdown() {
    super.shutdown();
    this.disconnect("");
    executorService.shutdown();
  }
}
