package org.pentaho.di.trans.kafka.producer;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


/**
 * Kafka Producer step processor
 *
 * @author Michael Spector
 */
public class KafkaProducer extends BaseStep implements StepInterface {

	public KafkaProducer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		KafkaProducerData data = (KafkaProducerData) sdi;
		if (data.producer != null) {
			data.producer.close();
			data.producer = null;
		}
		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		Object[] row = getRow();
		if (row == null) {
			setOutputDone();
			return false;
		}

		KafkaProducerMeta meta = (KafkaProducerMeta) smi;
		KafkaProducerData data = (KafkaProducerData) sdi;

		RowMetaInterface inputRowMeta = getInputRowMeta();

		if (first) {
			first = false;

			// Initialize Kafka client:
			if (data.producer == null) {
				Properties properties = meta.getKafkaProperties();
				Properties substProperties = new Properties();
				for (Entry<Object, Object> e : properties.entrySet()) {
					substProperties.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
				}
                substProperties.entrySet().forEach(entry -> logBasic("key: "+entry.getKey()+" value: "+entry.getValue()));
				org.apache.kafka.clients.producer.KafkaProducer producerConfig = new org.apache.kafka.clients.producer.KafkaProducer(substProperties);
				logBasic(Messages.getString("KafkaProducerStep.CreateKafkaProducer.Message",
						properties.getProperty("bootstrap.servers")));
				data.producer = producerConfig;
			}

			data.outputRowMeta = getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);

			int numErrors = 0;


			String keyField = environmentSubstitute(meta.getKeyField());

			if (!KafkaProducerMeta.isEmpty(keyField)) {
				logBasic(Messages.getString("KafkaProducerStep.Log.UsingKey", keyField));

				data.keyFieldNr = inputRowMeta.indexOfValue(keyField);

				if (data.keyFieldNr < 0) {
					logError(Messages.getString("KafkaProducerStep.Log.CouldntFindField", keyField)); //$NON-NLS-1$
					numErrors++;
				}
				if (!inputRowMeta.getValueMeta(data.keyFieldNr).isBinary()
						&& !inputRowMeta.getValueMeta(data.keyFieldNr).isString()) {
					logError(Messages.getString("KafkaProducerStep.Log.FieldNotValid", keyField)); //$NON-NLS-1$
					numErrors++;
				}
				data.keyIsString = inputRowMeta.getValueMeta(data.keyFieldNr).isString();
				data.keyFieldMeta = inputRowMeta.getValueMeta(data.keyFieldNr);

			} else {
				data.keyFieldNr = -1;
			}

			if (numErrors > 0) {
				setErrors(numErrors);
				stopAll();
				return false;
			}
		}

		try {

			String topic = environmentSubstitute(meta.getTopic());
			JSONObject jsonObject = getJsonString(inputRowMeta, row);
            JSONObject message = initFormMeta(meta,jsonObject);

            data.producer.send(new ProducerRecord<>(topic,message.toString()));

			incrementLinesOutput();
		} catch (Exception e) {
			if (!getStepMeta().isDoingErrorHandling()) {
				logError(Messages.getString("KafkaProducerStep.ErrorInStepRunning", e.getMessage()));
				setErrors(1);
				stopAll();
				setOutputDone();
				return false;
			}
			putError(getInputRowMeta(), row, 1, e.toString(), null, getStepname());
		}
		return true;
	}

	private JSONObject initFormMeta(KafkaProducerMeta meta, JSONObject json){
        //fields
		JSONObject jsonObject = new JSONObject();
        List<KafkaProducerMeta.Field> fields = meta.getFields();
        if(fields.isEmpty()){
            jsonObject.putAll(json);
        }else {
            json.entrySet().forEach(entry -> jsonObject.put(updateKey(entry.getKey(), fields), entry.getValue()));
        }

        //operation
        Map<String, String> operation = meta.getOperation();
        if(!operation.isEmpty()){
            jsonObject.put("op", operation.get("op"));
        }

        return jsonObject;
    }

    private String updateKey(String key, List<KafkaProducerMeta.Field> fields){
        List<KafkaProducerMeta.Field> collect = fields.stream().filter(field -> key.equals(field.sourceName))
                .collect(Collectors.toList());
        return collect.get(0).targetName;
    }


    private JSONObject getJsonString(RowMetaInterface inputRowMeta, Object[] row) {
        JSONObject json = new JSONObject();
        String[] fieldNames = inputRowMeta.getFieldNames();
        IntStream.range(0,fieldNames.length).forEach(i -> json.put(fieldNames[i],row[i]));

        return json;
    }

    public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		KafkaProducerData data = (KafkaProducerData) sdi;
		if (data.producer != null) {
			data.producer.close();
			data.producer = null;
		}
		super.stopRunning(smi, sdi);
	}
}
