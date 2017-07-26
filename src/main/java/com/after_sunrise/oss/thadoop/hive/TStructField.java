package com.after_sunrise.oss.thadoop.hive;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.thrift.TFieldIdEnum;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.builder.ToStringStyle.SHORT_PREFIX_STYLE;

/**
 * @author takanori.takase
 */
public class TStructField<F extends TFieldIdEnum> implements StructField {

    private final F field;

    private final byte type;

    private final ObjectInspector inspector;

    public TStructField(F field, byte type, ObjectInspector inspector) {
        this.field = checkNotNull(field);
        this.type = type;
        this.inspector = checkNotNull(inspector);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, SHORT_PREFIX_STYLE);
        builder.append("field", field);
        builder.append("type", type);
        builder.append("inspector", inspector.getClass().getSimpleName());
        return builder.toString();
    }

    public F getField() {
        return field;
    }

    public byte getType() {
        return type;
    }

    @Override
    public String getFieldName() {
        return field.getFieldName().toLowerCase();
    }

    @Override
    public String getFieldComment() {
        return toString();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
        return inspector;
    }

}
