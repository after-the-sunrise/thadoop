package com.after_sunrise.oss.thadoop.hive;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.TType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.builder.ToStringStyle.SHORT_PREFIX_STYLE;
import static org.apache.thrift.meta_data.FieldMetaData.getStructMetaDataMap;

/**
 * @author takanori.takase
 */
public class TObjectInspector<B extends TBase<?, ?>> extends
        StructObjectInspector {

    private final Class<B> clazz;

    private final List<StructField> fields;

    private final Map<String, StructField> fieldMap;

    public TObjectInspector(Class<B> clazz) {

        this.clazz = checkNotNull(clazz);

        this.fields = list(getStructMetaDataMap(clazz));

        this.fieldMap = mapFieldName(fields);

    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, SHORT_PREFIX_STYLE);
        builder.append("clazz", clazz.getSimpleName());
        return builder.toString();
    }

    @VisibleForTesting
    <F extends TFieldIdEnum> List<StructField> list(Map<F, FieldMetaData> fields) {

        List<StructField> list = new ArrayList<StructField>(fields.size());

        for (Entry<F, FieldMetaData> entry : fields.entrySet()) {

            F f = entry.getKey();

            FieldMetaData data = entry.getValue();

            FieldValueMetaData metaData = data.valueMetaData;

            ObjectInspector i = retrieveInspector(metaData);

            list.add(new TStructField<F>(f, metaData.type, i));

        }

        return list;

    }

    @VisibleForTesting
    ObjectInspector retrieveInspector(FieldValueMetaData metaData) {

        byte type = metaData.type;

        switch (type) {
            case TType.BOOL:
                return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            case TType.BYTE:
                return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
            case TType.DOUBLE:
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case TType.I16:
                return PrimitiveObjectInspectorFactory.javaShortObjectInspector;
            case TType.I32:
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case TType.I64:
                return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            case TType.STRING:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case TType.ENUM:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case TType.LIST:

                ListMetaData lmd = (ListMetaData) metaData;

                ObjectInspector li = retrieveInspector(lmd.elemMetaData);

                return ObjectInspectorFactory.getStandardListObjectInspector(li);

            case TType.MAP:

                MapMetaData mmd = (MapMetaData) metaData;

                ObjectInspector mk = retrieveInspector(mmd.keyMetaData);

                ObjectInspector mv = retrieveInspector(mmd.valueMetaData);

                return ObjectInspectorFactory.getStandardMapObjectInspector(mk, mv);

            case TType.SET:

                SetMetaData smd = (SetMetaData) metaData;

                ObjectInspector si = retrieveInspector(smd.elemMetaData);

                return ObjectInspectorFactory.getStandardListObjectInspector(si);

            case TType.STRUCT:

                StructMetaData stmd = (StructMetaData) metaData;

                @SuppressWarnings("rawtypes")
                Class<? extends TBase> stclz = stmd.structClass;

                @SuppressWarnings({"rawtypes", "unchecked"})
                ObjectInspector stoi = new TObjectInspector(stclz);

                return stoi;

        }

        throw new UnsupportedOperationException("Unsupported type : " + type);

    }

    @VisibleForTesting
    Map<String, StructField> mapFieldName(List<StructField> fields) {

        Map<String, StructField> map = new HashMap<String, StructField>();

        for (StructField field : fields) {

            String fieldName = field.getFieldName();

            map.put(fieldName, field);

        }

        return map;

    }

    @Override
    public Category getCategory() {
        return Category.STRUCT;
    }

    @Override
    public String getTypeName() {
        return clazz.getSimpleName();
    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
        return fieldMap.get(fieldName);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        return extract(data, fieldRef);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {

        List<Object> values = new ArrayList<Object>(fields.size());

        for (StructField field : fields) {

            Object value = extract(data, field);

            values.add(value);

        }

        return values;

    }

    @VisibleForTesting
    <F extends TFieldIdEnum> Object extract(Object data, StructField fieldRef) {

        @SuppressWarnings("unchecked")
        TBase<?, F> base = (TBase<?, F>) data;

        @SuppressWarnings("unchecked")
        TStructField<F> field = (TStructField<F>) fieldRef;

        if (!base.isSet(field.getField())) {
            return null;
        }

        Object value = base.getFieldValue(field.getField());

        if (value != null && field.getType() == TType.ENUM) {
            value = ((Enum<?>) value).name();
        }

        return value;

    }

}
