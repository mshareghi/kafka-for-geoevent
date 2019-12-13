package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.core.security.GeoEventServerCryptoService;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportDefinitionBase;
import com.esri.ges.transport.TransportType;

/**
 * Base class for the Kafka Transport Definitions
 */
abstract class KafkaTransportDefinitionBase extends TransportDefinitionBase {

    private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(KafkaTransportDefinitionBase.class);

    public static final String PROPERTY_BOOTSTRAP_SERVER = "bootstrapServer";
    public static final String PROPERTY_TOPIC = "topic";
    public static final String PROPERTY_ADVANCED_CONFIGS = "advancedConfigs";
    public static final String PROPERTY_ADVANCED_CONFIGS_FOLDER = "advancedConfigsFolder";
    public static final String PROPERTY_ADVANCED_CONFIGS_FILE_NAME = "advancedConfigsFileName";

    KafkaTransportDefinitionBase(TransportType type) {
        super(type);

        try {

            propertyDefinitions.put(PROPERTY_BOOTSTRAP_SERVER,
                new PropertyDefinition(PROPERTY_BOOTSTRAP_SERVER,
                    PropertyType.String,
                    "localhost:9092",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.BOOTSTRAP_SERVERS_LBL}",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.BOOTSTRAP_SERVERS_DESC}",
                    true,
                    false));

            propertyDefinitions.put(PROPERTY_TOPIC,
                new PropertyDefinition(PROPERTY_TOPIC,
                    PropertyType.String,
                    "",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.TOPIC_LBL}",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.TOPIC_DESC}",
                    true,
                    false));

            propertyDefinitions.put(PROPERTY_ADVANCED_CONFIGS,
                new PropertyDefinition(
                    PROPERTY_ADVANCED_CONFIGS,
                    PropertyType.String,
                    "",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.ADVANCED_CONFIGS_LBL}",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.ADVANCED_CONFIGS_DESC}",
                    "",
                    "textarea",
                    false,
                    false));

            propertyDefinitions.put(PROPERTY_ADVANCED_CONFIGS_FOLDER,
                new PropertyDefinition(
                    PROPERTY_ADVANCED_CONFIGS_FOLDER,
                    PropertyType.FolderDataStore,
                    "",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.ADVANCED_CONFIGS_FOLDER_LBL}",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.ADVANCED_CONFIGS_FOLDER_DESC}",
                    "",
                    "",
                    false,
                    false));

            propertyDefinitions.put(PROPERTY_ADVANCED_CONFIGS_FILE_NAME,
                new PropertyDefinition(
                    PROPERTY_ADVANCED_CONFIGS_FILE_NAME,
                    PropertyType.String,
                    "",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.ADVANCED_CONFIGS_FILE_NAME_LBL}",
                    "${com.esri.geoevent.transport.kafka-advanced-transport.ADVANCED_CONFIGS_FILE_NAME_DESC}",
                    "",
                    "",
                    false,
                    false));
        }
        catch (PropertyException e) {
            String errorMsg = LOGGER.translate("TRANSPORT_OUT_INIT_ERROR", e.getMessage());
            LOGGER.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }


    }
}
