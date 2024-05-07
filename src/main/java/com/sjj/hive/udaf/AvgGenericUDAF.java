package com.sjj.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author Jiajun Song
 * @version 1.0.0
 * @date 2024/5/7
 */
@Description(
        name = "avg_udaf_gen",
        value = "_FUNC_(x) - Returns the mean of a set of numbers",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(age) FROM table GROUP BY class;"
)
public class AvgGenericUDAF extends AbstractGenericUDAFResolver {
    /**
     * 根据入参获取对应的Evaluator执行器
     *
     * @param info
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentTypeException(info.length - 1,
                    "Exactly one argument is expected.");
        }

        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + info[0].getTypeName() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case VARCHAR:
            case CHAR:
            case TIMESTAMP:
                return new AvgGenericUDAFDoubleEvaluator();
            case DECIMAL:
            case BOOLEAN:
            case DATE:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type arguments are accepted but "
                                + info[0].getTypeName() + " is passed.");
        }
    }

    /**
     * 存储聚合过程中的中间数据
     */
    private static class AvgAggregationBuffer implements GenericUDAFEvaluator.AggregationBuffer {
        private long cnt = 0L;
        private double sum = 0.0;
    }

    /**
     * 针对Double类型的计算类，实现计算所需的方法
     */
    public static class AvgGenericUDAFDoubleEvaluator extends GenericUDAFEvaluator {
        /**
         * PARTIAL1及COMPLETE阶段的入参检查器
         */
        private transient PrimitiveObjectInspector inputOI;
        /**
         * PARTIAL2及FINAL阶段的入参检查器
         */
        private transient StructObjectInspector partialOI;
        private transient StructField countField;
        private transient StructField sumField;
        private LongObjectInspector countFieldOI;
        private DoubleObjectInspector sumFieldOI;
        /**
         * PARTIAL1及PARTIAL2阶段的中间结果
         */
        private transient Object[] partialResult;

        /**
         * 计算开始时调用，执行初始化相关操作
         *
         * @param m
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            // 初始化入参
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // 当前阶段为PARTIAL1或COMPLETE，则入参数据为Hive函数的入参
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                // 当前阶段为PARTIAL2或FINAL，则入参数据为中间结果
                partialOI = (StructObjectInspector) parameters[0];
                countField = partialOI.getStructFieldRef("count");
                sumField = partialOI.getStructFieldRef("sum");
                countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
                sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
                inputOI = (PrimitiveObjectInspector) partialOI.getStructFieldRef("input").getFieldObjectInspector();
            }

            // 初始化出参
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // 当前阶段为PARTIAL1或PARTIAL2，则出参数据为中间结果
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                // 将Hive函数原本的入参类型也记录下来
                // 若不在此处传递Hive函数原本的入参类型，则PARTIAL2阶段就无法从当前方法的入参中获取到Hive函数原本的入参类型
                foi.add(inputOI);
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("count");
                fname.add("sum");
                fname.add("input");
                partialResult = new Object[2];
                partialResult[0] = new LongWritable(0L);
                partialResult[1] = new DoubleWritable(0.0);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                // 当前阶段为COMPLETE或FINAL，则出参数据为Hive函数的出参
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        /**
         * 计算完成时调用，可用于关闭数据库连接等
         *
         * @throws IOException
         */
        @Override
        public void close() throws IOException {
            super.close();
        }

        /**
         * 获取新的中间数据存储实例
         *
         * @return
         * @throws HiveException
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new AvgAggregationBuffer();
        }

        /**
         * 对存储的中间数据进行重置
         *
         * @param agg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((AvgAggregationBuffer) agg).cnt = 0L;
            ((AvgAggregationBuffer) agg).sum = 0.0;
        }

        /**
         * 对Hive入参数据进行计算
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            Object parameter = parameters[0];
            if (null != parameter) {
                double value = PrimitiveObjectInspectorUtils.getDouble(parameter, inputOI);
                ((AvgAggregationBuffer) agg).cnt++;
                ((AvgAggregationBuffer) agg).sum += value;
            }
        }

        /**
         * 生成部分聚合结果
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ((LongWritable) partialResult[0]).set(((AvgAggregationBuffer) agg).cnt);
            ((DoubleWritable) partialResult[1]).set(((AvgAggregationBuffer) agg).sum);
            return partialResult;
        }

        /**
         * 对部分聚合结果进行合并
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (null != partial) {
                long partialCnt = countFieldOI.get(partialOI.getStructFieldData(partial, countField));
                double partialSum = sumFieldOI.get(partialOI.getStructFieldData(partial, sumField));
                ((AvgAggregationBuffer) agg).cnt += partialCnt;
                ((AvgAggregationBuffer) agg).sum += partialSum;
            }
        }

        /**
         * 计算最终结果
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            if (((AvgAggregationBuffer) agg).cnt == 0L) {
                return null;
            } else {
                return new DoubleWritable(((AvgAggregationBuffer) agg).sum / ((AvgAggregationBuffer) agg).cnt);
            }
        }
    }
}
