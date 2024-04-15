package com.sjj.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Jiajun Song
 * @version 1.0.0
 * @date 2024/4/15
 */
@Description(
        name = "get_max_num_gen",
        value = "_FUNC_(x, y, ...) - return the maximum value in x,y,...",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(1, 5) FROM table;\n"
                + "  5\n"
                + "  > SELECT _FUNC_(-123, 789, 0) FROM table;\n"
                + "  789"
)
public class GetMaxNumGenericUDF extends GenericUDF {
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes;
    private transient PrimitiveObjectInspector[] argumentOIs;
    private transient ObjectInspectorConverters.Converter[] inputConverters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException(
                    "get_max_num_gen requires at least 2 arguments, got " + arguments.length);
        }

        argumentOIs = new PrimitiveObjectInspector[arguments.length];
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
        inputConverters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException(
                        "get_max_num_gen takes only primitive types, got " + arguments[i].getTypeName());
            }
            argumentOIs[i] = (PrimitiveObjectInspector) arguments[i];

            inputTypes[i] = argumentOIs[i].getPrimitiveCategory();
            switch (inputTypes[i]) {
                case SHORT:
                case BYTE:
                case INT:
                    inputConverters[i] = ObjectInspectorConverters.getConverter(argumentOIs[i], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                    break;
                case LONG:
                    inputConverters[i] = ObjectInspectorConverters.getConverter(argumentOIs[i], PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                    break;
                case FLOAT:
                case STRING:
                case DOUBLE:
                    inputConverters[i] = ObjectInspectorConverters.getConverter(argumentOIs[i], PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                    break;
                case DECIMAL:
                    inputConverters[i] = ObjectInspectorConverters.getConverter(argumentOIs[i],
                            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                    ((PrimitiveObjectInspector) arguments[i]).getTypeInfo()));
                    break;
                default:
                    throw new UDFArgumentException(
                            "get_max_num_gen only takes SHORT/BYTE/INT/LONG/DOUBLE/FLOAT/STRING/DECIMAL types, got " + inputTypes[i]);
            }

        }
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Double result = null;
        for (int i = 0; i < arguments.length; i++) {
            Object valObject = arguments[i].get();
            if (valObject == null) {
                continue;
            }
            switch (inputTypes[i]) {
                case SHORT:
                case BYTE:
                case INT:
                    valObject = inputConverters[i].convert(valObject);
                    if (result == null || ((IntWritable) valObject).get() > result) {
                        result = (double) ((IntWritable) valObject).get();
                    }
                   break;
                case LONG:
                    valObject = inputConverters[i].convert(valObject);
                    if (result == null || ((LongWritable) valObject).get() > result) {
                        result = (double) ((LongWritable) valObject).get();
                    }
                    break;
                case FLOAT:
                case STRING:
                case DOUBLE:
                    valObject = inputConverters[i].convert(valObject);
                    if (result == null || ((DoubleWritable) valObject).get() > result) {
                        result = ((DoubleWritable) valObject).get();
                    }
                    break;
                case DECIMAL:
                    HiveDecimalObjectInspector decimalOI =
                            (HiveDecimalObjectInspector) argumentOIs[i];
                    HiveDecimalWritable val = decimalOI.getPrimitiveWritableObject(valObject);

                    if (result == null || val.doubleValue() > result) {
                        result = val.doubleValue();
                    }
                    break;
                default:
                    throw new UDFArgumentException(
                            "get_max_num_gen only takes SHORT/BYTE/INT/LONG/DOUBLE/FLOAT/STRING/DECIMAL types, got " + inputTypes[i]);
               }
           }
        return result == null ? null : new DoubleWritable(result);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "get_max_num_gen getDisplayString test";
    }
}
