package Detector;

import DataStructure.MCO;


import java.util.*;
import RPC.Vector;
import mtree.utils.MTreeClass;
import utils.Constants;

public class MCOD extends Detector {
    public HashMap<List<Double>, MCO> map_to_MCO;
    public Map<Integer, ArrayList<MCO>> internal_dataList; // {slideId, MCO}
    //--------------------------------------------------------------------------------
    public HashMap<MCO, ArrayList<MCO>> filled_clusters; // {d.center, cluster}
    public HashMap<MCO, ArrayList<MCO>> unfilled_clusters;
    // 全局的--------------------------------------------------------------
    public MTreeClass mtree;
    public HashSet<MCO> outliers;
    public PriorityQueue<MCO> eventQueue;

    public HashMap<List<Double>, Integer> external_info;

    //=========================for measurement=========================
    public int processExternalPoints = 0;
    //=========================mutiQuery=========================
    public int K;
    public double R;
    public double min_R;

    public MCOD() {
        super();

        K = Constants.K;
        R = Constants.R;
        min_R = Constants.R;

        map_to_MCO = new HashMap<>();
        internal_dataList = new LinkedHashMap<>();
        filled_clusters = new HashMap<>();
        unfilled_clusters = new HashMap<>();
        mtree = new MTreeClass();
        outliers = new HashSet<>();
        eventQueue = new PriorityQueue<MCO>(new Comparator<MCO>() {
            @Override
            public int compare(MCO o1, MCO o2) {
                return o1.ev - o2.ev;
            }
        });
        external_info = new HashMap<>();
    }

    public MCOD(int K, double min_R, double R) {
        super();

        this.K = K;
        this.R = R;
        this.min_R = min_R;
        System.out.println("mcod " + this.hashCode() + ": K is " + K + " min_R is " + min_R + " R is " + R);

        map_to_MCO = new HashMap<>();
        internal_dataList = new LinkedHashMap<>();
        filled_clusters = new HashMap<>();
        unfilled_clusters = new HashMap<>();
        mtree = new MTreeClass();
        outliers = new HashSet<>();
        eventQueue = new PriorityQueue<MCO>(new Comparator<MCO>() {
            @Override
            public int compare(MCO o1, MCO o2) {
                return o1.ev - o2.ev;
            }
        });
        external_info = new HashMap<>();
    }

    // 预处理入口函数
    @Override
    public void detectOutlier(List<Vector> data) {
        // 1.去除过期点
        if (Constants.S != Constants.nS) {
            // 1.1 除去internal过期的点
            for (Integer key : internal_dataList.keySet()) {
                if (key <= Constants.currentSlideID - Constants.nS) {
                    for (MCO d : internal_dataList.get(key)) {
                        if (d.isInFilledCluster) {
                            removeFromFilledCluster(d);
                        } else {
                            removeFromUnfilledCluster(d);
                        }
                        eventQueue.remove(d);
                    }
                }
            }
            internal_dataList.entrySet().removeIf(entry -> entry.getKey() <= Constants.currentSlideID - Constants.nS);

            // 1.2 除去external过期的点
            clean_expired_externalData();

        } else {
            internal_dataList.clear();
            external_info.clear();
            externalData.clear();
            filled_clusters.clear();
            unfilled_clusters.clear();
            eventQueue.clear();
            mtree = new MTreeClass();
            outliers.clear();
        }

        // 2.process new data
        internal_dataList.put(Constants.currentSlideID, new ArrayList<>());
        data.forEach(this::processNewData);

        process_event_queue();
        this.outlierVector = outliers;
    }

    private void removeFromFilledCluster(MCO d) {
        //get the cluster
        ArrayList<MCO> cluster = filled_clusters.get(d.center);
        if (cluster != null) {
            cluster.remove(d);
            filled_clusters.put(d.center, cluster);
            check_shrink(d.center);
        }

        List<Double> key = d.center.values;
        update_fingerprint(key, false);
    }

    private void removeFromUnfilledCluster(MCO d) {
        ArrayList<MCO> cluster = unfilled_clusters.get(d.center);
        if (cluster != null) {
            cluster.remove(d);
            List<Double> key = d.center.values;
            if (cluster.size() == 0) {
//                if (key.get(0)==12.45 && key.get(1)==9.77962) {
//                    System.out.println("disappear");
//                }
                unfilled_clusters.remove(d.center);
                map_to_MCO.remove(key);
                update_fingerprint(key, false);
                this.fullCellDelta.put(key, Constants.threadhold + this.fullCellDelta.get(key)); //不管有没有这个key，都可以实现覆盖效果
            } else {
                unfilled_clusters.put(d.center, cluster);
                update_fingerprint(key, false);
            }
        }
        // remove outlier中过期的点
        outliers.remove(d);

        //remove outlier中点的过期的前继
        outliers.forEach((data) -> {
            while (data.exps.size() > 0 && data.exps.get(0) <= d.slideID + Constants.nS) {
                data.exps.remove(0);
                if (data.exps.isEmpty()) {
                    data.ev = 0;
                } else {
                    data.ev = data.exps.get(0);
                }
            }
        });
    }

    private void resetObject(MCO o, boolean isInFilledCluster) {
        o.ev = 0;
        o.exps.clear();
        o.numberOfSucceeding = 0;
        o.isInFilledCluster = isInFilledCluster;
    }

    // 对于Unfilled cluster里的一个点d，更新它自己的前继后继和unfilled clusters里的前后继
    private void update_info_unfilled(MCO d, boolean fromShrinkCluster, boolean firstForm) {
        for (MCO center : unfilled_clusters.keySet()) {
            // 当fromFulledCluster并且遍历到这个shrink的cluster时，不需要重复计算
            if ((fromShrinkCluster||firstForm)&&center.equals(d.center)) continue;

            // 如果center中心离d 3R/2以内，检查这个unfilled cluster里的所有点
//            if (mtree.getDistanceFunction().calculate(center, d) <= 3 * Constants.R / 2) {
            if (mtree.getDistanceFunction().calculate(center, d) <= 3 * min_R / 2) {
                ArrayList<MCO> unfilled_cluster = unfilled_clusters.get(center);
                for (MCO point : unfilled_cluster) {
//                    if (point != d && mtree.getDistanceFunction().calculate(point, d) <= Constants.R) {
                    if (point != d && mtree.getDistanceFunction().calculate(point, d) <= min_R) {
                        if (isSameSlide(point, d) == -1) {
                            //p在d前面
                            d.exps.add(point.slideID + Constants.nS);
                            if (!fromShrinkCluster) {
                                // 如果是旧的点，那么point.numberOfSucceeding已经包含了这个d
                                point.updateSucceeding(d);
                            }
                        } else if (isSameSlide(point, d) == 0) {
                            //p和d在同一个slide
                            d.updateSucceeding(point);
                            if (!fromShrinkCluster) {
                                point.updateSucceeding(d);
                            }
                        } else {
                            //p在d后面
                            //对应一个filled_cluster变unfilled而言
                            d.updateSucceeding(point);
                            if (!fromShrinkCluster) {
                                point.exps.add(d.slideID + Constants.nS);
                            }
                        }

                        //just keep k-numberOfSucceedingNeighbor
                        if (!fromShrinkCluster) {
                            //对于from shrink的点，point其实没有更新
                            checkInlier(point);
                        }
                    }
                }
            }
        }
    }

    //d这个点在filled_cluster里找邻居更新信息
    private void update_info_filled(MCO d) {
        for (MCO center : filled_clusters.keySet()) {
            // 如果center中心离d 3R/2以内，检查这个filled cluster里的所有点
//            if (mtree.getDistanceFunction().calculate(center, d) <= 3 * Constants.R / 2) {
            if (mtree.getDistanceFunction().calculate(center, d) <= 3 * min_R / 2) {
                ArrayList<MCO> filled_cluster = filled_clusters.get(center);
                for (MCO point : filled_cluster) {
//                    if (mtree.getDistanceFunction().calculate(point, d) <= Constants.R) {
                    if (mtree.getDistanceFunction().calculate(point, d) <= min_R) {
                        if (isSameSlide(d, point) <= 0) {
                            //p is succeeding neighbor
                            d.updateSucceeding(point);
                        } else {
                            //p is preceding neighbor
                            d.exps.add(point.slideID + Constants.nS);
                        }
                    }
                }
            }
        }
        checkInlier(d);
    }

    private void addToUnfilledCluster(MCO nearest_center, MCO d) {
        //更新点的信息
        d.isCenter = false;
        d.isInFilledCluster = false;
        d.center = nearest_center;

        //更新cluster信息
        ArrayList<MCO> cluster = unfilled_clusters.get(nearest_center);
        cluster.add(d);
        unfilled_clusters.put(nearest_center, cluster);

        //update fullCellDelta
        List<Double> key = d.center.values;
        update_fingerprint(key, true);

        //这两步顺序不能换，因为是在update_info_filled里checkInlier(d)
        //update self and others succeeding and preceding in unfilled_cluster
        update_info_unfilled(d, false, false);
        //find neighbors in filled clusters (3R/2)
        update_info_filled(d);

        check_filled(nearest_center);
    }

    private void formUnfilledCluster(MCO d) {
        d.isCenter = true;
        d.isInFilledCluster = false;
        d.center = d;
        map_to_MCO.put(d.values, d);

        ArrayList<MCO> cluster = new ArrayList<>();
        cluster.add(d);
        unfilled_clusters.put(d, cluster);

        this.fullCellDelta.put(d.center.values, 1);

        update_info_unfilled(d, false,true);
        update_info_filled(d);

    }

    private void check_filled(MCO center) {
        ArrayList<MCO> cluster = unfilled_clusters.get(center);
//        if (cluster.size() > Constants.K) {
        if (cluster.size() > K) {
            unfilled_clusters.remove(center);
            filled_clusters.put(center, cluster);
            cluster.forEach(p -> {
                outliers.remove(p);
                eventQueue.remove(p);
                resetObject(p, true);
            });
        }
    }

    private void check_shrink(MCO center) {
        ArrayList<MCO> cluster = filled_clusters.get(center);
//        if (cluster.size() <= Constants.K) {
        if (cluster.size() <= K) {
            // 表明变成了unfilled cluster，更新cluster的状态
            filled_clusters.remove(center);
            unfilled_clusters.put(center, cluster);

            //更新cluster里点的状态
            cluster.sort(new MCComparatorSlideId());
            for (int i = 0; i < cluster.size(); i++) {
                MCO o = cluster.get(i);
                resetObject(o, false);
                //先处理下自己cluster里的,算preceding和succeeding
                for (int j = 0; j < i; j++) {
                    MCO n = cluster.get(j);
                    o.exps.add(n.slideID + Constants.nS);
                }
                o.numberOfSucceeding = cluster.size() - 1 - i;

                update_info_unfilled(o, true,false);
                update_info_filled(o);
            }
        }
    }

    private void addToFilledCluster(MCO nearest_center, MCO d) {
        //更新点的信息
        d.isCenter = false;
        d.isInFilledCluster = true;
        d.center = nearest_center;

        //更新cluster的信息
        ArrayList<MCO> cluster = filled_clusters.get(nearest_center);
        cluster.add(d);
        filled_clusters.put(nearest_center, cluster);

        // update fingerprint
        List<Double> key = d.center.values;
        update_fingerprint(key, true);

        //update for points in PD that has Rmc list contains center
        // filled 里的自己不用存succeeding和preceding，只用更新unfilled里的点的邻居
        for (MCO center : unfilled_clusters.keySet()) {
            // 如果center中心离d 3R/2以内，检查这个unfilled cluster里的所有点
//            if (mtree.getDistanceFunction().calculate(center, d) <= 3 * Constants.R / 2) {
            if (mtree.getDistanceFunction().calculate(center, d) <= 3 * min_R / 2) {
                ArrayList<MCO> unfilled_cluster = unfilled_clusters.get(center);
                for (MCO point : unfilled_cluster) {
//                    if (mtree.getDistanceFunction().calculate(point, d) <= Constants.R) {
                    if (mtree.getDistanceFunction().calculate(point, d) <= min_R) {
                        //好像没有这种情况了？
                        if (isSameSlide(d, point) == -1) {
                            point.exps.add(d.slideID + Constants.nS);
                        } else {
                            point.updateSucceeding(d);
                        }
                        //check if point become inlier
                        checkInlier(point);
                    }
                }
            }
        }
    }

    public MCO findNearestCenter(MCO d, boolean filled) {
        HashMap<MCO, ArrayList<MCO>> cluster;
        if (filled) cluster = filled_clusters;
        else cluster = unfilled_clusters;

        double min_distance = Double.MAX_VALUE;
        MCO min_center_id = null;
        for (MCO center : cluster.keySet()) {
            //compute the distance
            double distance = mtree.getDistanceFunction().calculate(center, d);

            if (distance < min_distance) {
                min_distance = distance;
                min_center_id = center;
            }
        }
        return min_center_id;
    }

    private void processNewData(Vector vector) {

        MCO d = new MCO(vector);

        //add to datalist
        internal_dataList.get(Constants.currentSlideID).add(d);

        // 1.是否可加入filled_cluster
        MCO nearest_filled_center = findNearestCenter(d, true);
        double min_distance = Double.MAX_VALUE;
        if (nearest_filled_center != null) { //found the nearest cluster
            min_distance = mtree.getDistanceFunction().calculate(nearest_filled_center, d);
        }
        //assign to cluster if min distance <= R/2
//        if (min_distance <= Constants.R / 2) {
        if (min_distance <= min_R / 2) {
            addToFilledCluster(nearest_filled_center, d);
        } else {
            // 2.是否可加入unfilled_cluster
            MCO nearest_unfilled_center_id = findNearestCenter(d, false);
            min_distance = Double.MAX_VALUE;
            if (nearest_unfilled_center_id != null) { //found the nearest cluster
                min_distance = mtree.getDistanceFunction().calculate(nearest_unfilled_center_id, d);
            }
//            if (min_distance <= Constants.R / 2) {
            if (min_distance <= min_R / 2) {
                addToUnfilledCluster(nearest_unfilled_center_id, d);
            } else {
                // 3.自己成一个unfilled_cluster
                formUnfilledCluster(d);
            }
        }
    }

    // 保留最少数目proceeding
    // 检查是否为safe | 剩余inlier（queue） | outlier
    private void checkInlier(MCO inPD, Iterator<MCO> iterator) {
        Collections.sort(inPD.exps);

//        while (inPD.exps.size() > Constants.K - inPD.numberOfSucceeding && inPD.exps.size() > 0) {
        while (inPD.exps.size() > K - inPD.numberOfSucceeding && inPD.exps.size() > 0) {
            inPD.exps.remove(0);
        }
        if (inPD.exps.size() > 0) inPD.ev = inPD.exps.get(0);
        else inPD.ev = 0;

//        if (inPD.exps.size() + inPD.numberOfSucceeding >= Constants.K) {
        if (inPD.exps.size() + inPD.numberOfSucceeding >= K) {
//            if (inPD.numberOfSucceeding >= Constants.K) {
            if (inPD.numberOfSucceeding >= K) {
                eventQueue.remove(inPD);
                iterator.remove();
            } else {
                iterator.remove();
                if (!eventQueue.contains(inPD)) {
                    eventQueue.offer(inPD);
                }
                else {
                    eventQueue.remove(inPD);
                    eventQueue.offer(inPD);
                }
            }
        } else {
            eventQueue.remove(inPD);
            outliers.add(inPD);
        }
    }

    private void checkInlier(MCO inPD) {
        Collections.sort(inPD.exps);

//        while (inPD.exps.size() > Constants.K - inPD.numberOfSucceeding && inPD.exps.size() > 0) {
        while (inPD.exps.size() > K - inPD.numberOfSucceeding && inPD.exps.size() > 0) {
            inPD.exps.remove(0);
        }
        if (inPD.exps.size() > 0) inPD.ev = inPD.exps.get(0);
        else inPD.ev = 0;

//        if (inPD.exps.size() + inPD.numberOfSucceeding >= Constants.K) {
        if (inPD.exps.size() + inPD.numberOfSucceeding >= K) {
//            if (inPD.numberOfSucceeding >= Constants.K) {
            if (inPD.numberOfSucceeding >= K) {
                eventQueue.remove(inPD);
                outliers.remove(inPD);
            } else {
                outliers.remove(inPD);
                if (!eventQueue.contains(inPD)) {
                    eventQueue.offer(inPD);
                }
                else {
                    eventQueue.remove(inPD);
                    eventQueue.offer(inPD);
                }
            }
        } else {
            eventQueue.remove(inPD);
            outliers.add(inPD);
        }
    }

    private void process_event_queue() {
        MCO x = eventQueue.peek();
        while (x != null && x.ev <= Constants.currentSlideID) {
            x = eventQueue.poll();
//            if (x.values.get(0) == 11.757){
//                System.out.println("MCOD pre: " + x.exps.size());
//                System.out.println("MCOD sud: " + x.numberOfSucceeding);
//                for (int i = 0; i < x.exps.size(); i++) {
//                    System.out.print(x.exps.get(i) + " ");
//                }
//            }
            while (x.exps.size() != 0 && x.ev <= Constants.currentSlideID) { //@shimin

                x.exps.remove(0);
                if (x.exps.isEmpty()) {
                    x.ev = 0;
                    break;
                } else
                    x.ev = x.exps.get(0);
            }
//            if (x.exps.size() + x.numberOfSucceeding < Constants.K)
            if (x.exps.size() + x.numberOfSucceeding < K)
                outliers.add(x);
//            else if (x.numberOfSucceeding < Constants.K && x.exps.size() + x.numberOfSucceeding >= Constants.K){
            else if (x.numberOfSucceeding < K && x.exps.size() + x.numberOfSucceeding >= K){
                eventQueue.add(x);
            }

            x = eventQueue.peek();
        }
    }

    public void processOutliers() {
//        System.out.printf("Thead %d processOutliers. \n", Thread.currentThread().getId());
        processExternalPoints = 0;
        update_external_info();
        check_local_outliers();
        this.outlierVector = outliers;
    }

    public boolean neighboringTupleSet(List<Double> v1, List<Double> v2, double threshold) {

        double ss = 0;
        threshold *= threshold;
        for (int i = 0; i < v2.size(); i++) {
            ss += Math.pow((v1.get(i) - v2.get(i)), 2);
            if (ss > threshold) return false;
        }
        return true;
    }
    public void clean_expired_externalData() {
        //Map<Integer, Map<ArrayList<?>, List<Vector>>> externalData;
        // 去除上个过期时间的所有点
        externalData.remove(Constants.currentSlideID - Constants.nS);

        Iterator<Integer> it_time = externalData.keySet().iterator();
        while (it_time.hasNext()) {
            //每个时间点
            Integer time = it_time.next();
            Map<List<Double>, List<Vector>> clusters = externalData.get(time);
            Iterator<List<Double>> it_cluster = clusters.keySet().iterator();
            while (it_cluster.hasNext()) {
                //每个cluster
                List<Double> key = it_cluster.next();
                Iterator<Vector> it_vector = clusters.get(key).iterator();//实例化迭代器
                while (it_vector.hasNext()) {
                    //每个vector
                    Vector v = it_vector.next();//读取当前集合数据元素
                    if (v.slideID <= Constants.currentSlideID - Constants.nS) {
                        int cnt = external_info.get(key);
                        if (cnt == 1) {
                            external_info.remove(key);
                        } else {
                            external_info.put(key, cnt - 1);
                        }
                        it_vector.remove();
                    }
                }
                if (clusters.get(key).size() == 0) {
                    it_cluster.remove();
                }
            }
            if (externalData.get(time).size() == 0) {
                it_time.remove();
            }
        }
    }

    //更新external_info至最新状态
    public void update_external_info() {
        // Map<Integer, Map<ArrayList<?>, List<Vector>>> externalData;
        Map<List<Double>, List<Vector>> current_arrive_data = externalData.get(Constants.currentSlideID);
        if (current_arrive_data == null) {
            return;
        }
        for (List<Double> key : current_arrive_data.keySet()) {
            if (!external_info.containsKey(key)) {
                external_info.put(key, 0);
            }
            int cnt = external_info.get(key);
            external_info.put(key, cnt + current_arrive_data.get(key).size());
        }
    }

    public void check_local_outliers() {
//        System.out.printf("Thead %d check_local_outliers. \n", Thread.currentThread().getId());
        Iterator<MCO> iterator = outliers.iterator();//实例化迭代器
        outlierLoop:
        while (iterator.hasNext()) {
            MCO o = iterator.next();//读取当前集合数据元素
            // HashMap<ArrayList<?>, Integer> status;
            if (!this.status.containsKey(o.center.values)){
                System.out.println(o.center.values);
            }
            int reply = this.status.get(o.center.values);
            //首先我们需要pruning掉被判断为安全的以及被判断成outlier的点，加入event queue，event time 设为下一个时间点
            if (reply == 2) {
                iterator.remove();
                // 是在device端就确定为inlier的情况,没有精确的最早的neighbor过期的时间 更新不了相应proceeding和succeeding
                o.ev = Constants.currentSlideID + 1;
                eventQueue.add(o);
            }

            // todo#: 传回信息多一个数量，嘚在getExternalData里面接收一下，后面找个位置更新

            //确定为outlier的点不用做操作
            //不确定的点：
            // 从离他1/2R中的cluster的点之和是否大于K，如果大于K，加入event queue，event time 设为下一个时间点
            // 如果不大于K，从离他3/2R中的cluster的点之和是否小于K，如果小于，则就是outlier
            // 如果大于K，就进行具体的距离计算，更新pre，succeeding,last_calculated_time
            else if (reply == 1) {
                ArrayList<List<Double>> cluster3R_2 = new ArrayList<>();
                for (Map.Entry<List<Double>, Integer> entry : external_info.entrySet()) {
                    List<Double> key = entry.getKey();
                    double distance = distance(key, o.values);
//                    if (distance <= Constants.R * 3 / 2) {
                    if (distance <= R * 3 / 2) {
                        cluster3R_2.add(key);
                    }
                }

                if (o.last_calculate_time == -1 || o.last_calculate_time <= Constants.currentSlideID - Constants.nS) {
                    o.last_calculate_time = Constants.currentSlideID - Constants.nS + 1;
                }
                while (o.last_calculate_time <= Constants.currentSlideID) {
                    Map<List<Double>, List<Vector>> cur_data = externalData.get(o.last_calculate_time);
                    if (cur_data != null) {
                        // 对每一个3R/2内的邻居
                        for (List<Double> c : cluster3R_2) {
                            // 在当前的时间点看是否有此cluster
                            List<Vector> cur_cluster_data = cur_data.get(c);
                            // 如果有
                            if (cur_cluster_data != null) {
                                processExternalPoints += cur_cluster_data.size();
                                for (Vector v : cur_cluster_data) {
//                                    if (distance(v.values, o.values) <= Constants.R) {
                                    if (distance(v.values, o.values) <= R) {
                                        if (isSameSlide(o, v) <= 0) {
                                            o.updateSucceeding(v);
                                        } else {
                                            //p is preceding neighbor
                                            o.exps.add(v.slideID + Constants.nS);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    o.last_calculate_time++;

                    checkInlier(o, iterator);
//                    if (o.numberOfSucceeding + o.exps.size() >= Constants.K) {
                    if (o.numberOfSucceeding + o.exps.size() >= K) {
                        continue outlierLoop;
                    }
                }
            }
        }
//        try {
//            EdgeNodeNetwork.ratioInfo.write("Client # process external points / # outliers = " + processExternalPoints * 1.0 / outliers.size()+"\n");
//            EdgeNodeNetwork.ratioInfo.flush();
//            EdgeNodeNetwork.ratioInfoCSV.write(processExternalPoints * 1.0 / outliers.size()+",");
//            EdgeNodeNetwork.ratioInfoCSV.flush();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }



    @Override
    public Map<List<Double>, List<Vector>> sendData(Set<List<Double>> bucketIds, int deviceHashCode) {
        Map<List<Double>, List<Vector>> result = new HashMap<>();
        for (List<Double> bucketId: bucketIds){
            if (!historyRecord.containsKey(bucketId)){
                historyRecord.put(bucketId, Collections.synchronizedMap(new HashMap<>()));
            }
            Map<Integer,Integer> bucketHistory = historyRecord.get(bucketId);
            if (!bucketHistory.containsKey(deviceHashCode)){
                bucketHistory.put(deviceHashCode, -1);
            }
            int lastSent = Math.max(bucketHistory.get(deviceHashCode), Constants.currentSlideID - Constants.nS);
            bucketHistory.put(deviceHashCode, Constants.currentSlideID);
            for (int time = lastSent + 1; time <= Constants.currentSlideID; time++) {
                for (MCO dataPoints : internal_dataList.get(time)) {
                    List<Double> d_center = dataPoints.center.values;
                    if (d_center.equals(bucketId)) {
                        if (result.containsKey(d_center))
                            result.get(d_center).add(dataPoints);
                        else {
                            List<Vector> vectors = new ArrayList<>();
                            vectors.add(dataPoints);
                            result.put(d_center, vectors);
                        }
                    }
                }
            }
        }
        return result;
    }

    public double distance(List<Double> a, List<Double> b) {
        double sum = 0;
        for (int i = 0; i < a.size(); i++) {
            sum += Math.pow(a.get(i) - b.get(i), 2);
        }
        return Math.sqrt(sum);
    }

    public double distance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(sum);
    }

//    static class MCComparator implements Comparator<MCO> {
//        @Override
//        public int compare(MCO o1, MCO o2) {
//            return o1.ev - o2.ev;
//        }
//    }

    static class MCComparatorSlideId implements Comparator<MCO> {
        @Override
        public int compare(MCO o1, MCO o2) {
            return Integer.compare(o1.slideID, o2.slideID);
        }
    }

    public int isSameSlide(Vector o1, Vector o2) {
        return Integer.compare(o1.slideID, o2.slideID);
    }

    public void update_fingerprint(List<Double> key, boolean isAdd) {
        if (!this.fullCellDelta.containsKey(key)) {
            this.fullCellDelta.put(key, 0);
        }
        int origin = this.fullCellDelta.get(key);
        int delta = isAdd ? 1 : -1;
        this.fullCellDelta.put(key, origin + delta);
    }
}
