package com.sjj.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * @author Jiajun Song
 * @version 1.0.0
 * @date 2024/5/7
 */
@Description(
        name = "avg_udaf",
        value = "_FUNC_(x) - Returns the mean of a set of numbers",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(age) FROM table GROUP BY class;"
)
@SuppressWarnings("deprecation")
public class AvgUDAF extends UDAF {
    /**
     * 创建一个类，用于存储计算所需的数据
     */
    private static class AvgState {
        private double sum = 0.0;
        private long cnt = 0L;
    }

    /**
     * 执行计算的内部类，Hive会自动找到在当前UDAF函数内部实现UDAFEvaluator接口的内部类
     */
    public static class AvgUDAFEvaluator implements UDAFEvaluator {

        AvgState avgState = null;

        public AvgUDAFEvaluator() {
            super();
            avgState = new AvgState();
            init();
        }

        /**
         * 重置存储的数据
         */
        @Override
        public void init() {
            avgState.cnt = 0L;
            avgState.sum = 0.0;
        }

        /**
         * 遍历输入Hive的数据并进行处理，若入参合规且计算正确，则应返回true，否则抛出异常
         * 在该函数中，为计算入参总和并计数
         *
         * @param num 输入Hive的数据，需要跟函数入参一致
         * @return
         * @throws Exception
         */
        public boolean iterate(Double num) throws Exception {
            if (null == avgState) {
                throw new HiveException("The avgState must not be null!");
            }

            if (null != num) {
                avgState.cnt += 1L;
                avgState.sum += num;
            }
            return true;
        }

        /**
         * 当一次部分聚合结束时，返回部分聚合的中间结果，用于后续计算
         *
         * @return
         */
        public AvgState terminatePartial() {
            // 按照SQL标准，当入参数量为0时，返回值为null
            return avgState.cnt == 0L ? null : avgState;
        }

        /**
         * 将部分聚合的结果进行合并
         * 当计算正常时，应当返回true
         *
         * @param other
         * @return
         */
        public boolean merge(AvgState other) {
            if (null != other) {
                avgState.cnt += other.cnt;
                avgState.sum += other.sum;
            }
            return true;
        }

        /**
         * 全部聚合结束，返回最终结果
         *
         * @return
         */
        public Double terminate() {
            return avgState.cnt == 0L ? null : avgState.sum / avgState.cnt;
        }
    }

    private AvgUDAF() {
        // 避免实例化
    }
}
