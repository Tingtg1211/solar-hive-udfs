package com.envisioniot.hive.udfs;

import com.envisioniot.hive.entity.Line;
import com.envisioniot.hive.entity.Point;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


@Description(name = "calc_prod",
        value = "_FUNC_(timestamp, kwh, slope, dateFormat, lastValidTime, lastValidValue) - Returns an map of related production values in the aggregation group "
)
public class CalcProdUDAF extends AbstractGenericUDAFResolver {
    public static final Logger LOG = Logger.getLogger(CalcProdUDAF.class);
    public static final String SLOPE = "slope";
    public static final String DATEFORMAT = "dateFormat";
    public static final String LASTVALIDTIME = "lastValidTime";
    public static final String LASTVALIDVALUE = "lastValidValue";
    public static final String PRODUCTION = "production";
    public static final String TIMEGROUP = "timeGroup";

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        return new CalcProdUDAFEvaluator();
    }

    public static class CalcProdUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data

        // PrimitiveObjectInspector 针对数据库列的属性基本类型
        private PrimitiveObjectInspector inputKeyOI;
        private PrimitiveObjectInspector inputValOI;

        private DoubleObjectInspector slopeOI;
        // 定义日期分组格式
        private StringObjectInspector dateFormatOI;
        private StringObjectInspector lastValidTimeOI;
        private DoubleObjectInspector lastValidValueOI;
        private ObjectInspector outputOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of objs)
        private StandardMapObjectInspector internalMergeOI;

        public CalcProdUDAFEvaluator() {
        }

        class CalcProdAggBuffer implements AggregationBuffer {
            private Map<Object, Object> map = new HashMap<Object, Object>();

            public void addValue(Object keyObj, Object valObj) {
                map.put(inputKeyOI.copyObject(keyObj).toString(), inputValOI.copyObject(valObj).toString());
            }

            public void addSlope(Object slopeObj) {
                map.put(SLOPE, slopeOI.copyObject(slopeObj).toString());
            }

            public void addDateFormat(Object dateFormatObj) {
                map.put(DATEFORMAT, dateFormatOI.copyObject(dateFormatObj).toString());
            }

            public void addLastValidTime(Object lastValidTimeObj) {
                if (lastValidTimeObj != null) {
                    map.put(LASTVALIDTIME, lastValidTimeOI.copyObject(lastValidTimeObj).toString());
                }
            }

            public void addLastValidValue(Object lastValidValueObj) {
                if (lastValidValueObj != null) {
                    map.put(LASTVALIDVALUE, lastValidValueOI.copyObject(lastValidValueObj).toString());
                }
            }


            public Map<Object, Object> getValueMap() {
                return map;
            }

            public void reset() {
                map.clear();
            }
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            LOG.info(" CalcProdUDAF.init() - Mode= " + m.name());
            for (int i = 0; i < parameters.length; ++i) {
                LOG.info(" ObjectInspector[ " + i + " ] = " + parameters[i]);
            }
            if (m == Mode.PARTIAL1) {
                if (parameters.length != 4 && parameters.length != 6) {
                    throw new HiveException("4 or 6 parameters required, current is " + parameters.length);
                }
                if (parameters[2] instanceof DoubleObjectInspector) {
                    slopeOI = (DoubleObjectInspector) parameters[2];
                } else {
                    throw new HiveException("Slope must be a constant double.");
                }
                if (parameters[3] instanceof StringObjectInspector) {
                    //TODO 校验dateFormatOI的格式只能为yyyy-MM-dd 或者yyyy-MM 或者yyyy
                    dateFormatOI = (StringObjectInspector) parameters[3];
                } else {
                    throw new HiveException("DateFormat must be a constant string");
                }
                if (parameters.length == 6) {
                    if (parameters[4] instanceof StringObjectInspector) {
                        lastValidTimeOI = (StringObjectInspector) parameters[4];
                    } else {
                        throw new HiveException("Last valid time must be a constant string.");
                    }
                    if (parameters[5] instanceof DoubleObjectInspector) {
                        lastValidValueOI = (DoubleObjectInspector) parameters[5];
                    } else {
                        throw new HiveException("Last valid value must be a constant double.");
                    }
                }
            }

            // init output object inspectors
            // The output of a partial aggregation is a map
            if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                inputValOI = (PrimitiveObjectInspector) parameters[1];
            } else {
                internalMergeOI = (StandardMapObjectInspector) parameters[0];
                inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                inputValOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
            }
            if (m == Mode.FINAL) {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardMapObjectInspector(
                                ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                                ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));

            }

            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                    ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CalcProdAggBuffer buff = new CalcProdAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object k = parameters[0];
            Object v = parameters[1];
            if (k == null || v == null) {
                throw new HiveException("Key or value is null.  k = " + k + " , v = " + v);
            }

            if (k != null) {
                CalcProdAggBuffer myagg = (CalcProdAggBuffer) agg;

                if (parameters.length > 3) {
                    myagg.addSlope(parameters[2]);
                    myagg.addDateFormat(parameters[3]);
                    if (parameters.length == 6) {
                        myagg.addLastValidTime(parameters[4]);
                        myagg.addLastValidValue(parameters[5]);
                    }
                }

                putIntoSet(k, v, myagg);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            CalcProdAggBuffer myagg = (CalcProdAggBuffer) agg;
            Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);
            for (Object i : partialResult.keySet()) {
                putIntoSet(i, partialResult.get(i), myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            CalcProdAggBuffer arrayBuff = (CalcProdAggBuffer) buff;
            arrayBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException{
            CalcProdAggBuffer myagg = (CalcProdAggBuffer) agg;
            Map result = myagg.getValueMap();

            List<String> otherItems = Arrays.asList(SLOPE, DATEFORMAT, LASTVALIDTIME, LASTVALIDVALUE);
            Point lastValidPoint = null; // 存放最后一个有效点

            Map<String/*format后的时间格式*/, List<Point>> prodGroupMap = new TreeMap<String, List<Point>>(); //记录按时间格式分组后的电量数据 ，TreeMap自动根据字符串字典顺序排序
            List<Point> preDayList = new ArrayList<Point>(); // 记录最后一个有效点
            List<Map<String, String>> calcResult = new ArrayList<Map<String, String>>(); // 存放最终的每组计算值
            String dateFormat = result.get(DATEFORMAT).toString();
            SimpleDateFormat formatter= new SimpleDateFormat(dateFormat);
            Double slopeValue = Double.valueOf(result.get(SLOPE).toString());

            // 将收集到的数据转换成Point类型并存放到指定的时间格式分组map里
            for (Object i : result.keySet()) {
                if (otherItems.contains(i.toString())) {
                    continue;
                }
              // 将字符串类型的时间转换成Date类型
                Date date = null;
                try {
                    date = formatter.parse(i.toString());
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                String timeGroup = formatter.format(date);  // 按照传入的指定分组格式，格式化时间
                Point point = convertPoint(result, i); // 将时间戳-电量数据转换成Point类型

                List<Point> pointList = prodGroupMap.get(timeGroup);
                if (pointList == null) {
                    pointList = new ArrayList<Point>();
                }
                if (point != null) {
                    pointList.add(point);
                    prodGroupMap.put(timeGroup, pointList);
                }

            }


            // 如果存在传入的上一天最后一个有效点则转换成Point类型并存放
            if (result.get(LASTVALIDTIME) != null && result.get(LASTVALIDVALUE) != null) {
                lastValidPoint = convertPoint(result, null);
            }

            // 对分好组的电量点进行计算，并输出最后一个有效点
            for (Map.Entry<String, List<Point>> entry : prodGroupMap.entrySet()) {
                String dateTime = entry.getKey();
                List<Point> pointList = entry.getValue();
                Point preDayPoint = null ;

                // 添加第一个有效点即上天的最后一个有效点
                if (preDayList != null && !preDayList.isEmpty()) {
                    preDayPoint = preDayList.get(preDayList.size() - 1); // 取最后一个有效点，list里已经保证时间有序，因为是按时间顺序添加的
                } else {
                    preDayList = new ArrayList<Point>();
                    preDayPoint = lastValidPoint;
                }
                if (preDayPoint != null) {
                    pointList.add(preDayPoint);
                }
                Collections.sort(pointList); // 对所有点按照时间排序

                // 计算时间分组的电量值及最后一个有效点
                Map<String, String> resultMap = calcProdAlgorithm(dateTime, pointList, slopeValue, preDayPoint);
                // 如果存在有效点加入有效点容器中,没有有效点沿用之前的有效点
                if (resultMap.get(LASTVALIDTIME) != null && resultMap.get(LASTVALIDVALUE) != null) {
                    preDayList.add(convertPoint(resultMap, null));
                }
                calcResult.add(resultMap);

            }
            return calcResult;

        }


        private void putIntoSet(Object key, Object val, CalcProdAggBuffer myagg) {
            myagg.addValue(key, val);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            CalcProdAggBuffer myagg = (CalcProdAggBuffer) agg;
            Map<Object, Object> vals = myagg.getValueMap();
            return vals;
        }

        private Point convertPoint(Map value, Object flag) {
            Point point = new Point();
            if (flag == null) {
                String dateValue = value.get(LASTVALIDTIME).toString();
                Double prodValue = Double.valueOf(value.get(LASTVALIDVALUE).toString());
                point.setDate(dateValue);
                point.setProd(prodValue);
            } else {
                point.setDate(flag.toString());
                point.setProd(Double.valueOf(value.get(flag).toString()));
            }
            return point;
        }

        private Map<String, String> calcProdAlgorithm(String date, List<Point> pointList, Double slopeValue, Point preDayPoint) {
            Double prod = 0d;
            Point lastValidPoint = new Point();
            if (pointList != null && !pointList.isEmpty()) {
                List<Line> solid = new ArrayList<Line>();//实线段
                List<Line> dotted = new ArrayList<Line>();//虚线段
                Line dottedLine = null;
                Line solidLine = null;
                //1. 分出虚实线
                for (int i = 0; i < pointList.size() - 1; i++) {
                    Point first = pointList.get(i);
                    Point second = pointList.get(i + 1);
                    Line line = new Line(first, second);
                    double slope = line.getSlope();
                    if (slope > 0 && slope < slopeValue) {//斜率正常
                        if (solidLine == null) {
                            solidLine = new Line();
                            solidLine.setFirst(first);
                        }
                        solidLine.setSecond(second);
                        if (i == pointList.size() - 2) {
                            solid.add(solidLine);
                        }
                        if (dottedLine != null) {
                            dotted.add(dottedLine);
                        }
                        dottedLine = null;
                    } else {//斜率不正常
                        if (dottedLine == null) {
                            dottedLine = new Line();
                            dottedLine.setFirst(first);
                        }
                        dottedLine.setSecond(second);
                        if (i == pointList.size() - 2) {
                            dotted.add(dottedLine);
                        }
                        if (solidLine != null) {
                            solid.add(solidLine);
                        }
                        solidLine = null;
                    }
                }

                // 2. 连接每天有效虚线 (判断上步连续跳突连接的首尾虚线斜率是否正常，正常的话标识为实线)
                int dottedSize = dotted.size();
                int solidSize = solid.size();

                if (dotted != null && !dotted.isEmpty()) {
                    for (Line line : dotted) {
                        double slope = line.getSlope();
                        if (slope > 0 && slope < slopeValue) {//斜率正常
                            solid.add(line);
                        } else if (slope == 0d) {//死数
                            dottedSize--;
                        } else {//跳变

                        }
                    }
                }

                // 3. 计算有效发电量
                for (Line line : solid) {
                    prod += line.getProd();
                }

                // 4. 存在有效点,输出最后一个有效点，将作为下一天的第一个有效点
                if (!solid.isEmpty()) {
                    Collections.sort(solid);
                    lastValidPoint = solid.get(solid.size() - 1).getSecond();
                } else {//当天不存在有效点
                    //沿用前一天的有效点，将作为下一天的第一个有效点
                    if (preDayPoint != null) {
                        lastValidPoint = preDayPoint;
                    }
                }

            }

            // 5. 输出计算结果
            Map<String, String> resultMap = new HashMap<String, String>();
            resultMap.put(TIMEGROUP, date);
            resultMap.put(PRODUCTION, prod.toString());
            resultMap.put(LASTVALIDTIME, lastValidPoint.getDate());
            if (lastValidPoint.getProd() == null) {
                resultMap.put(LASTVALIDVALUE, null);
            } else {
                resultMap.put(LASTVALIDVALUE, lastValidPoint.getProd().toString());
            }
            return resultMap;
        }
    }

}
