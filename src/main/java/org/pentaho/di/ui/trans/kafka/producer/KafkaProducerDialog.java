package org.pentaho.di.ui.trans.kafka.producer;

import java.awt.*;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.*;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import org.pentaho.di.trans.kafka.producer.KafkaProducerMeta;
import org.pentaho.di.trans.kafka.producer.Messages;
import org.pentaho.ui.xul.swing.tags.SwingRadio;
import org.pentaho.ui.xul.swt.tags.SwtRadioGroup;

/**
 * UI for the Kafka Producer step
 *
 * @author Michael Spector
 */
public class KafkaProducerDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = KafkaProducerMeta.class;

	private CTabFolder wTabFolder;
	private FormData fdTabFolder;
	private CTabItem wGeneralTab;
    private Composite wGeneralComp;
	private FormData fdGeneralComp;

	private String[] fieldNames;
    private ModifyListener lsMod;

	private CTabItem wFieldsTab;
    private TableView wFields;

    private CTabItem wOperationTab;
    private Group opGroup;
    private Map<String, String> op = new HashMap<>();

	private KafkaProducerMeta producerMeta;
	private TextVar wTopicName;
	private CCombo wKeyField;
	private TableView wProps;

	public KafkaProducerDialog(Shell parent, Object in, TransMeta tr, String sname) {
		super(parent, (BaseStepMeta) in, tr, sname);
		producerMeta = (KafkaProducerMeta) in;
	}

	public KafkaProducerDialog(Shell parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname) {
		super(parent, baseStepMeta, transMeta, stepname);
		producerMeta = (KafkaProducerMeta) baseStepMeta;
	}

	public KafkaProducerDialog(Shell parent, int nr, BaseStepMeta in, TransMeta tr) {
		super(parent, nr, in, tr);
		producerMeta = (KafkaProducerMeta) in;
	}

	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, producerMeta);

		lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				producerMeta.setChanged();
			}
		};
		changed = producerMeta.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(Messages.getString("KafkaProducerDialog.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Step name
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(Messages.getString("KafkaProducerDialog.StepName.Label"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);
		Control lastControl = wStepname;

		// Topic name
		Label wlTopicName = new Label(shell, SWT.RIGHT);
		wlTopicName.setText(Messages.getString("KafkaProducerDialog.TopicName.Label"));
		props.setLook(wlTopicName);
		FormData fdlTopicName = new FormData();
		fdlTopicName.top = new FormAttachment(lastControl, margin);
		fdlTopicName.left = new FormAttachment(0, 0);
		fdlTopicName.right = new FormAttachment(middle, -margin);
		wlTopicName.setLayoutData(fdlTopicName);
		wTopicName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTopicName);
		wTopicName.addModifyListener(lsMod);
		FormData fdTopicName = new FormData();
		fdTopicName.top = new FormAttachment(lastControl, margin);
		fdTopicName.left = new FormAttachment(middle, 0);
		fdTopicName.right = new FormAttachment(100, 0);
		wTopicName.setLayoutData(fdTopicName);
		lastControl = wTopicName;

		RowMetaInterface previousFields;
		try {
			previousFields = transMeta.getPrevStepFields(stepMeta);
		} catch (KettleStepException e) {
			new ErrorDialog(shell, BaseMessages.getString("System.Dialog.Error.Title"),
					Messages.getString("KafkaProducerDialog.ErrorDialog.UnableToGetInputFields"), e);
			previousFields = new RowMeta();
		}

		// Key Field
		Label wlKeyField = new Label(shell, SWT.RIGHT);
		wlKeyField.setText(Messages.getString("KafkaProducerDialog.KeyFieldName.Label"));
		props.setLook(wlKeyField);
		FormData fdlKeyField = new FormData();
		fdlKeyField.top = new FormAttachment(lastControl, margin);
		fdlKeyField.left = new FormAttachment(0, 0);
		fdlKeyField.right = new FormAttachment(middle, -margin);
		wlKeyField.setLayoutData(fdlKeyField);
		wKeyField = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wKeyField.setItems(previousFields.getFieldNames());
		props.setLook(wKeyField);
		wKeyField.addModifyListener(lsMod);
		FormData fdKeyField = new FormData();
		fdKeyField.top = new FormAttachment(lastControl, margin);
		fdKeyField.left = new FormAttachment(middle, 0);
		fdKeyField.right = new FormAttachment(100, 0);
		wKeyField.setLayoutData(fdKeyField);
		lastControl = wKeyField;

        wTabFolder = new CTabFolder( shell, SWT.BORDER );
        props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

		// GENERAL TAB
		addGeneralTab();

		// Fields TAB
		addFieldsTab();

		// Operation TAB
        addOperationTab();

		// Buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString("System.Button.OK")); //$NON-NLS-1$
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString("System.Button.Cancel")); //$NON-NLS-1$

		setButtonPositions(new Button[] { wOK, wCancel }, margin, null);

        fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment( 0, 0 );
        fdTabFolder.top = new FormAttachment( lastControl, margin );
        fdTabFolder.right = new FormAttachment( 100, 0 );
        fdTabFolder.bottom = new FormAttachment( wOK, -margin );
        wTabFolder.setLayoutData( fdTabFolder );

		// Add listeners
		addStandardListeners();

        wTabFolder.setSelection( 0 );

		// Set the shell size, based upon previous time...
        setSize(shell, 400, 350, true);
		getData(producerMeta, true);
		producerMeta.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return stepname;
	}

    private void addOperationTab() {
        wOperationTab = new CTabItem( wTabFolder, SWT.NONE );
        wOperationTab.setText( BaseMessages.getString( PKG, "KafkaProducerDialog.Operation.Tab" ) );

        RowLayout rowLayout = new RowLayout(SWT.VERTICAL);
        rowLayout.marginLeft = 40;
        rowLayout.marginTop = 20;
        rowLayout.spacing = 20;

        opGroup = new Group(wTabFolder, SWT.NONE);
        opGroup.setLayout(rowLayout);

        Button buttonInsert = new Button(opGroup, SWT.RADIO);
        buttonInsert.setText("Insert");

        Button buttonUpdate = new Button(opGroup, SWT.RADIO);
        buttonUpdate.setText("Update");

        Button buttonDelete = new Button(opGroup, SWT.RADIO);
        buttonDelete.setText("Delete");

        SelectionListener selectionListener = new SelectionAdapter () {
            public void widgetSelected(SelectionEvent event) {
                Button button = (Button) event.widget;
                String operation = StringUtils.isEmpty(button.getText()) ? "Insert" : button.getText();
                op.put("op", operation);
            }
        };

        buttonInsert.addSelectionListener(selectionListener);
        buttonUpdate.addSelectionListener(selectionListener);
        buttonDelete.addSelectionListener(selectionListener);

        opGroup.layout();
        wOperationTab.setControl(opGroup);

    }

    private void addGeneralTab() {
		wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
		wGeneralTab.setText( BaseMessages.getString( PKG, "KafkaProducerDialog.General.Tab" ) );

        FormLayout generalLayout = new FormLayout();
        generalLayout.marginWidth = Const.FORM_MARGIN;
        generalLayout.marginHeight = Const.FORM_MARGIN;

        wGeneralComp = new Composite( wTabFolder, SWT.NONE );
        wGeneralComp.setLayout( generalLayout );
        props.setLook( wGeneralComp );

        // Kafka properties
        ColumnInfo[] columnInfo = new ColumnInfo[] {
                new ColumnInfo(Messages.getString("KafkaProducerDialog.TableView.NameCol.Label"),
                        ColumnInfo.COLUMN_TYPE_TEXT, false),
                new ColumnInfo(Messages.getString("KafkaProducerDialog.TableView.ValueCol.Label"),
                        ColumnInfo.COLUMN_TYPE_TEXT, false), };

        wProps = new TableView(transMeta, wGeneralComp,SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnInfo, 1, lsMod, props);
        FormData fdProps = new FormData();
        fdProps.top = new FormAttachment(0, Const.MARGIN);
        fdProps.left = new FormAttachment(0,  Const.MARGIN);
        fdProps.bottom = new FormAttachment(100, 0);
        fdProps.right = new FormAttachment(100, 0);
        wProps.setLayoutData(fdProps);


		fdGeneralComp = new FormData();
        fdGeneralComp.top = new FormAttachment( 0, 0 );
        fdGeneralComp.left = new FormAttachment( 0, 0 );
        fdGeneralComp.bottom = new FormAttachment( 100, 0 );
        fdGeneralComp.right = new FormAttachment( 100, 0 );
		wGeneralComp.setLayoutData( fdGeneralComp );

		wGeneralComp.layout();
		wGeneralTab.setControl( wGeneralComp );
	}

    private void addFieldsTab() {

        wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
        wFieldsTab.setText( BaseMessages.getString( PKG, "KafkaProducerDialog.FieldsTab.TabTitle" ) );

        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginWidth = Const.FORM_MARGIN;
        fieldsLayout.marginHeight = Const.FORM_MARGIN;

        Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
        wFieldsComp.setLayout( fieldsLayout );
        props.setLook( wFieldsComp );

        wGet = new Button( wFieldsComp, SWT.PUSH );
        wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
        wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );

        lsGet = new Listener() {
            public void handleEvent( Event e ) {
                getPreviousFields( wFields );
            }
        };
        wGet.addListener( SWT.Selection, lsGet );

        setButtonPositions( new Button[]{ wGet }, Const.MARGIN, null );

        final int fieldsRowCount = producerMeta.getFields().size();

        String[] names = this.fieldNames != null ? this.fieldNames : new String[]{ "" };
        ColumnInfo[] columnsMeta = new ColumnInfo[2];
        columnsMeta[0] =
                new ColumnInfo(
                        BaseMessages.getString( PKG, "KafkaProducerDialog.SourceNameColumn.Column" ),
                        ColumnInfo.COLUMN_TYPE_CCOMBO, names, false );
        columnsMeta[1] =
                new ColumnInfo(
                        BaseMessages.getString( PKG, "KafkaProducerDialog.TargetNameColumn.Column" ),
                        ColumnInfo.COLUMN_TYPE_TEXT, false );

        wFields =
                new TableView(
                        transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columnsMeta, fieldsRowCount,
                        lsMod, props );

        FormData fdFields = new FormData();
        fdFields.left = new FormAttachment( 0, Const.MARGIN );
        fdFields.top = new FormAttachment( 0, Const.MARGIN );
        fdFields.right = new FormAttachment( 100, -Const.MARGIN );
        fdFields.bottom = new FormAttachment( wGet, -Const.MARGIN );
        wFields.setLayoutData( fdFields );

        FormData fdFieldsComp = new FormData();
        fdFieldsComp.left = new FormAttachment( 0, 0 );
        fdFieldsComp.top = new FormAttachment( 0, 0 );
        fdFieldsComp.right = new FormAttachment( 100, 0 );
        fdFieldsComp.bottom = new FormAttachment( 100, 0 );
        wFieldsComp.setLayoutData( fdFieldsComp );

        wFieldsComp.layout();
        wFieldsTab.setControl( wFieldsComp );
    }

    private void getPreviousFields( TableView table ) {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields( stepname );
            if ( r != null ) {
                BaseStepDialog.getFieldsFromPrevious( r, table, 1, new int[]{ 1, 2 }, null, 0, 0, null );
            }
        } catch ( KettleException ke ) {
            new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
                    .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
        }
    }

	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	private void getData(KafkaProducerMeta producerMeta, boolean copyStepname) {
		if (copyStepname) {
			wStepname.setText(stepname);
		}
		wTopicName.setText(Const.NVL(producerMeta.getTopic(), ""));
		wKeyField.setText(Const.NVL(producerMeta.getKeyField(), ""));

		//general
		TreeSet<String> propNames = new TreeSet<String>();
		propNames.addAll(Arrays.asList(KafkaProducerMeta.KAFKA_PROPERTIES_NAMES));
		propNames.addAll(producerMeta.getKafkaProperties().stringPropertyNames());

		Properties kafkaProperties = producerMeta.getKafkaProperties();
		int i = 0;
		for (String propName : propNames) {
			String value = kafkaProperties.getProperty(propName);
			TableItem item = new TableItem(wProps.table, i++ > 1 ? SWT.BOLD : SWT.NONE);
			int colnr = 1;
			item.setText(colnr++, Const.NVL(propName, ""));
			String defaultValue = KafkaProducerMeta.KAFKA_PROPERTIES_DEFAULTS.get(propName);
			if (defaultValue == null) {
				defaultValue = "(default)";
			}
			item.setText(colnr++, Const.NVL(value, defaultValue));
		}

		wProps.removeEmptyRows();
		wProps.setRowNums();
		wProps.optWidth(true);

        // Fields
        mapToTableView( producerMeta.getFieldsMap(), wFields );

        //operation
        Map<String, String> operation = producerMeta.getOperation();
        String op = operation.get("op")==null ? "Insert": operation.get("op");
        Control[] children = opGroup.getChildren();
        for(Control c : children){
            Button button = (Button) c;
            if(op.equals(button.getText()))
                button.setSelection(true);
        }
        wStepname.selectAll();
	}

	private void cancel() {
		stepname = null;
		producerMeta.setChanged(changed);
		dispose();
	}

	/**
	 * Copy information from the dialog fields to the meta-data input
	 */
	private void setData(KafkaProducerMeta producerMeta) {
		producerMeta.setTopic(wTopicName.getText());
		producerMeta.setKeyField(wKeyField.getText());

		//general
		Properties kafkaProperties = producerMeta.getKafkaProperties();
		int nrNonEmptyFields = wProps.nrNonEmpty();
		for (int i = 0; i < nrNonEmptyFields; i++) {
			TableItem item = wProps.getNonEmpty(i);
			int colnr = 1;
			String name = item.getText(colnr++);
			String value = item.getText(colnr++).trim();
			if (value.length() > 0 && !"(default)".equals(value)) {
				kafkaProperties.put(name, value);
			} else {
				kafkaProperties.remove(name);
			}
		}
		wProps.removeEmptyRows();
		wProps.setRowNums();
		wProps.optWidth(true);

		//fields
        producerMeta.clearFields();
        for ( int i = 0; i < wFields.getItemCount(); i++ ) {
            String[] row = wFields.getItem( i );
            producerMeta.addField( row[0], row[1] );
        }

        //operation
        Control[] children = opGroup.getChildren();
        for(Control c : children){
            Button button = (Button) c;
            if(button.getSelection()){
                op.put("op", button.getText());
            }
        }
        producerMeta.setOperation(op);
		producerMeta.setChanged();
	}

	private void ok() {
		if (KafkaProducerMeta.isEmpty(wStepname.getText())) {
			return;
		}
		setData(producerMeta);
		stepname = wStepname.getText();
		dispose();
	}

    private void mapToTableView(Map<String, String> map, TableView table ) {
        for ( String key : map.keySet() ) {
            table.add( key, map.get( key ) );
        }
        table.removeEmptyRows();
        table.setRowNums();
    }

    private void addStandardListeners() {
	    lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};

        lsMod = new ModifyListener() {
            public void modifyText( ModifyEvent event ) {
                producerMeta.setChanged();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};
		wStepname.addSelectionListener(lsDef);
		wTopicName.addSelectionListener(lsDef);
		wKeyField.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});


    }
}
