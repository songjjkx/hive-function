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

import java.io.IOException;

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
    /**
     * 输入参数类型
     */
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes;
    /**
     * 输入参数对象检查器
     */
    private transient PrimitiveObjectInspector[] argumentOIs;
    /**
     * 输入参数对象转换器
     */
    private transient ObjectInspectorConverters.Converter[] inputConverters;

    /**
     * 初始化函数，对参数类型及数量等进行检查
     * 若函数需要连接数据库或发送HTTP请求等，可在此处执行创建数据库连接或建立连接池等操作
     *
     * @param arguments
     *          The ObjectInspector for the arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException(
                    "get_max_num_gen requires at least 2 arguments, got " + arguments.length);
        }
        // 输入参数的相关信息
        argumentOIs = new PrimitiveObjectInspector[arguments.length];
        // 输入参数类型
        inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
        // 输入参数对应的类型转换器
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

    /**
     * 执行计算流程
     *
     * @param arguments
     *          The arguments as DeferedObject, use DeferedObject.get() to get the
     *          actual argument Object. The Objects can be inspected by the
     *          ObjectInspectors passed in the initialize call.
     * @return
     * @throws HiveException
     */
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
//                    HiveDecimalObjectInspector decimalOI =
//                            (HiveDecimalObjectInspector) argumentOIs[i];
//                    HiveDecimalWritable val = decimalOI.getPrimitiveWritableObject(valObject);

                    valObject = inputConverters[i].convert(valObject);
                    HiveDecimalWritable val = (HiveDecimalWritable) valObject;

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

    /**
     * 函数执行结束时调用该方法
     * 可在此处执行关闭数据库连接等操作
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        super.close();
    }

    /**
     * @param children  入参字段名称
     * @return  使用explain查看执行计划时，该函数的输出内容
     */
    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("test-get_max_num_gen", children);
    }
}
