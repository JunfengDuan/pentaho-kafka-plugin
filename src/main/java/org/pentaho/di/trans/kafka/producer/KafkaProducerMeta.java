package org.pentaho.di.trans.kafka.producer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import org.pentaho.di.core.annotations.Step;

/**
 * Kafka Producer step definitions and serializer to/from XML and to/from Kettle
 * repository.
 *
 * @author Michael Spector
 */
@Step(	
		id = "KafkaProducer",
		image = "org/pentaho/di/trans/kafka/producer/resources/kafka_producer.png",
		i18nPackageName="org.pentaho.di.trans.kafka.producer",
		name="KafkaProducerDialog.Shell.Title",
		description = "KafkaProducerDialog.Shell.Tooltip",
		categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Output")
public class KafkaProducerMeta extends BaseStepMeta implements StepMetaInterface {

	public static final String[] KAFKA_PROPERTIES_NAMES = new String[] { "bootstrap.servers", "client.id",
			"acks", "retries", "timeout.ms", "batch.size","linger.ms","buffer.memory",
            "key.serializer","value.serializer"
    };

	public static final Map<String, String> KAFKA_PROPERTIES_DEFAULTS = new HashMap<>();
	static {
		KAFKA_PROPERTIES_DEFAULTS.put("bootstrap.servers", "localhost:9092");
        KAFKA_PROPERTIES_DEFAULTS.put("client.id", "0");
		KAFKA_PROPERTIES_DEFAULTS.put("acks", "1");
        KAFKA_PROPERTIES_DEFAULTS.put("retries", "0");
        KAFKA_PROPERTIES_DEFAULTS.put("batch.size", "16384");
        KAFKA_PROPERTIES_DEFAULTS.put("linger.ms", "1");
        KAFKA_PROPERTIES_DEFAULTS.put("buffer.memory", "33554432");//32M
        KAFKA_PROPERTIES_DEFAULTS.put("timeout.ms", "30000");
        KAFKA_PROPERTIES_DEFAULTS.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KAFKA_PROPERTIES_DEFAULTS.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	private Properties kafkaProperties = new Properties();
	private String topic;
	private String keyField;
    private List<Field> fields = new ArrayList<>();

    public Map<String, String> getOperation() {
        return operation;
    }

    public void setOperation(Map<String, String> operation) {
        this.operation = operation;
    }

    private Map<String, String> operation = new HashMap<>();
	public Properties getKafkaProperties() {
		return kafkaProperties;
	}

	/**
	 * @return Kafka topic name
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * @param topic
	 *            Kafka topic name
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * @return Target key field name in Kettle stream
	 */
	public String getKeyField() {
		return keyField;
	}

	/**
	 * @param field
	 *            Target key field name in Kettle stream
	 */
	public void setKeyField(String field) {
		this.keyField = field;
	}

    public List<Field> getFields() {
        return Collections.unmodifiableList( fields );
    }

    public Map<String, String> getFieldsMap() {
        Map<String, String> result = new TreeMap<>();
        for ( Field f : fields ) {
            result.put( f.sourceName, f.targetName );
        }
        return Collections.unmodifiableMap( result );
    }

    public void setFieldsMap( Map<String, String> values ) {
        clearFields();
        for ( String k : values.keySet() ) {
            addField( k, values.get( k ) );
        }
    }

    public void clearFields() {
        this.fields.clear();
    }

    public void addField( String inputName, String nameInJson ) {
        Field f = new Field();
        f.sourceName = inputName;
        f.targetName = StringUtils.isBlank( nameInJson ) ? inputName : nameInJson;
        this.fields.add( f );
    }


	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
					  String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore) {

		if (isEmpty(topic)) {
			remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
					Messages.getString("KafkaProducerMeta.Check.InvalidTopic"), stepMeta));
		}

		try {
			new org.apache.kafka.clients.producer.KafkaProducer(kafkaProperties);
		} catch (IllegalArgumentException e) {
			remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, e.getMessage(), stepMeta));
		}
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans trans) {
		return new KafkaProducer(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new KafkaProducerData();
	}

	/* This function adds meta data to the rows being pushed out */
	public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
						   VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
		if ( StringUtils.isNotBlank( this.getKeyField() ) ) {
			ValueMetaInterface valueMeta =
					new ValueMeta( space.environmentSubstitute( this.getKeyField()), ValueMetaInterface.TYPE_STRING );
			valueMeta.setOrigin( name );
			// add if doesn't exist
			if ( !r.exists( valueMeta ) ) {
				r.addValueMeta( valueMeta );
			}
		}
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
			throws KettleXMLException {

		try {
			topic = XMLHandler.getTagValue(stepnode, "TOPIC");
			keyField = XMLHandler.getTagValue(stepnode, "KEYFIELD");

            // General
			Node general = XMLHandler.getSubNode(stepnode, "KAFKA");
			String[] kafkaElements = XMLHandler.getNodeElements(general);
			if (kafkaElements != null) {
				for (String propName : kafkaElements) {
					String value = XMLHandler.getTagValue(general, propName);
					if (value != null) {
						kafkaProperties.put(propName, value);
					}
				}
			}

            // Fields
            Node fields = XMLHandler.getSubNode( stepnode, Dom.TAG_FIELDS );
            int nrFields = XMLHandler.countNodes( fields, Dom.TAG_FIELD );
            this.clearFields();
            for ( int i = 0; i < nrFields; i++ ) {
                Node fNode = XMLHandler.getSubNodeByNr( fields, Dom.TAG_FIELD, i );

                String colName = XMLHandler.getTagValue( fNode, Dom.TAG_NAME );
                String targetName = XMLHandler.getTagValue( fNode, Dom.TAG_TARGET );

                this.addField( colName, targetName );
            }

            //operation
            Node op = XMLHandler.getSubNode( stepnode, Dom.TAG_OPERATION);
            if(op != null){
                String[] ops = XMLHandler.getNodeElements(op);
                if(ops != null){
                    Arrays.asList(ops).forEach(propName ->{
                        String value = XMLHandler.getTagValue(op, propName);
                        operation.put(Dom.TAG_OP, value);
                    } );
                }
            }


		} catch (Exception e) {
			throw new KettleXMLException(Messages.getString("KafkaProducerMeta.Exception.loadXml"), e);
		}
	}

	public String getXML() throws KettleException {
		StringBuilder retval = new StringBuilder();

		if (topic != null) {
			retval.append("    ").append(XMLHandler.addTagValue("TOPIC", topic));
		}

		if (keyField != null) {
			retval.append("    ").append(XMLHandler.addTagValue("KEYFIELD", keyField));
		}

        // General
        retval.append("    ").append( XMLHandler.openTag("KAFKA") ).append( Const.CR );

		for (String name : kafkaProperties.stringPropertyNames()) {
			String value = kafkaProperties.getProperty(name);
			if (value != null) {
				retval.append("      " + XMLHandler.addTagValue(name, value));
			}
		}
		retval.append("    ").append(XMLHandler.closeTag("KAFKA")).append(Const.CR);

        // Fields
        retval.append("    ").append( XMLHandler.openTag( Dom.TAG_FIELDS ) ).append( Const.CR );
        for ( Field f : fields ) {
            retval.append("    ").append( XMLHandler.openTag( Dom.TAG_FIELD ) ).append( Const.CR );
            retval.append("    ").append( XMLHandler.addTagValue( Dom.TAG_NAME, f.sourceName ) );
            retval.append("    ").append( XMLHandler.addTagValue( Dom.TAG_TARGET, f.targetName ) );
            retval.append("    ").append( XMLHandler.closeTag( Dom.TAG_FIELD ) ).append( Const.CR );
        }
        retval.append("    ").append( XMLHandler.closeTag( Dom.TAG_FIELDS ) ).append( Const.CR );

        //operation
        retval.append("    ").append( XMLHandler.openTag( Dom.TAG_OPERATION ) ).append( Const.CR );
        String op = operation.get(Dom.TAG_OP) == null ? "Insert" : operation.get(Dom.TAG_OP);
        retval.append("    ").append( XMLHandler.addTagValue( Dom.TAG_OP,  op) );
        retval.append("    ").append( XMLHandler.closeTag( Dom.TAG_OPERATION ) ).append( Const.CR );
		return retval.toString();
	}

	public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			topic = rep.getStepAttributeString(stepId, "TOPIC");
			keyField = rep.getStepAttributeString(stepId, "KEYFIELD");
			String kafkaPropsXML = rep.getStepAttributeString(stepId, "KAFKA");

			if (kafkaPropsXML != null) {
				kafkaProperties.loadFromXML(new ByteArrayInputStream(kafkaPropsXML.getBytes()));
			}
			// Support old versions:
			for (String name : KAFKA_PROPERTIES_NAMES) {
				String value = rep.getStepAttributeString(stepId, name);
				if (value != null) {
					kafkaProperties.put(name, value);
				}
			}

            // Fields
            clearFields();
            int fieldsNr = rep.countNrStepAttributes( stepId, joinRepAttr( Dom.TAG_FIELD, Dom.TAG_NAME ) );
            for ( int i = 0; i < fieldsNr; i++ ) {
                String name = rep.getStepAttributeString( stepId, i, joinRepAttr( Dom.TAG_FIELD, Dom.TAG_NAME ) );
                String target = rep.getStepAttributeString( stepId, i, joinRepAttr( Dom.TAG_FIELD, Dom.TAG_TARGET ) );
                addField( name, target );
            }

            //operation
            String op = rep.getStepAttributeString(stepId, Dom.TAG_OP);
            operation.put(Dom.TAG_OP,op);
        } catch (Exception e) {
			throw new KettleException("KafkaProducerMeta.Exception.loadRep", e);
		}
	}

	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId) throws KettleException {
		try {
			if (topic != null) {
				rep.saveStepAttribute(transformationId, stepId, "TOPIC", topic);
			}

			if (keyField != null) {
				rep.saveStepAttribute(transformationId, stepId, "KEYFIELD", keyField);
			}
			ByteArrayOutputStream buf = new ByteArrayOutputStream();
			kafkaProperties.storeToXML(buf, null);
			rep.saveStepAttribute(transformationId, stepId, "KAFKA", buf.toString());


            // Fields
            for ( int i = 0; i < fields.size(); i++ ) {
                rep.saveStepAttribute( transformationId, stepId, i, joinRepAttr( Dom.TAG_FIELD, Dom.TAG_NAME ), fields.get(
                        i ).sourceName );
                rep.saveStepAttribute( transformationId, stepId, i, joinRepAttr( Dom.TAG_FIELD, Dom.TAG_TARGET ), fields.get(
                        i ).targetName );
            }

            //operation
            if(!operation.isEmpty()){
                rep.saveStepAttribute(transformationId, stepId, Dom.TAG_OPERATION, Dom.TAG_OP);
                rep.saveStepAttribute(transformationId, stepId, Dom.TAG_OPERATION, operation.get(Dom.TAG_OP));
            }
            ///////////////////////////
            ///////同步模型数据/////////
            /////////////////////////

		} catch (Exception e) {
			throw new KettleException("KafkaProducerMeta.Exception.saveRep", e);
		}
	}

	public void setDefault() {

	}
    private static String joinRepAttr( String... args ) {
        return StringUtils.join( args, "_" );
    }


    public static boolean isEmpty(String str) {
		return str == null || str.length() == 0;
	}

    public static class Field {
        @Injection( name = "SOURCE_NAME" )
        public String sourceName;
        @Injection( name = "TARGET_NAME" )
        public String targetName;
    }

    /**
     * Serialization aids
     */
    private static class Dom {
        static final String TAG_GENERAL = "general";
        static final String TAG_FIELDS = "fields";
        static final String TAG_FIELD = "field";

        static final String TAG_NAME = "columnName";
        static final String TAG_TARGET = "targetName";

        static final String TAG_OPERATION="operation";
        static final String TAG_OP="op";

    }
}


