package Framework;

import Detector.*;
import Handler.*;
import RPC.*;
import RPC.Vector;
import org.apache.thrift.TException;
import utils.Constants;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeNodeImpl implements EdgeNodeService.Iface {
    public EdgeNode belongedNode;
    public Map<List<Double>, List<UnitInNode>> unitResultInfo; //primly used for pruning
    public Map<List<Double>, UnitInNode> unitsStatusMap; // used to maintain the status of the unit in a node
    public Handler handler;
    public Map<Integer, EdgeNodeService.Client> clientsForEdgeNodes;
    public Map<Integer, DeviceService.Client> clientsForDevices;
    /* this two fields are used for judge whether the node has complete uploading*/
    public AtomicInteger count;
    volatile Boolean flag = false;

    //=============================Centralize===============================
    public Detector detector;
    public List<Vector> allData;
    public List<Vector> rawData;

    //========================for multiple query========================
    public int minK;
    public int maxK ;
    public double minR;
    public double maxR;
    public AtomicInteger count_RK = new AtomicInteger(0);

    public EdgeNodeImpl(EdgeNode edgeNode) {
        this.belongedNode = edgeNode;
        this.unitsStatusMap = new ConcurrentHashMap<>();
        this.unitResultInfo = new ConcurrentHashMap<>();
        this.allData = Collections.synchronizedList(new ArrayList<>());
        this.rawData = Collections.synchronizedList(new ArrayList<>());
        this.count = new AtomicInteger(0);
        if (Constants.methodToGenerateFingerprint.contains("NETS")) {
            this.handler = new NETSHandler(this);
        } else if (Constants.methodToGenerateFingerprint.contains("MCOD")) {
            this.handler = new MCODHandler(this);
        }

        if (Constants.isVariousR){
            minR = Double.MAX_VALUE;
            maxR = Double.MIN_VALUE;
        }
        else {
            minR = Constants.R;
            maxR = Constants.R;
        }
        if (Constants.isVariousK){
            minK = Integer.MAX_VALUE;
            maxK = Integer.MIN_VALUE;
        }
        else {
            minK = Constants.K;
            maxK = Constants.K;
        }

        // for baseline
        if (Objects.equals(Constants.methodToGenerateFingerprint, "NETS_CENTRALIZE")) {
            this.detector = new NewNETS(0);
        } else if (Objects.equals(Constants.methodToGenerateFingerprint, "MCOD_CENTRALIZE")) {
            this.detector = new MCOD();
        }
    }

    public void setClients(Map<Integer, EdgeNodeService.Client> clientsForEdgeNodes, Map<Integer, DeviceService.Client> clientsForDevices) {
        this.clientsForDevices = clientsForDevices;
        this.clientsForEdgeNodes = clientsForEdgeNodes;
    }

    public void receiveAndProcessFP(Map<List<Double>, Integer> fingerprints, int edgeDeviceHashCode) {
//        System.out.printf("Thead %d receiveAndProcessFP. \n", Thread.currentThread().getId());
        for (List<Double> id : fingerprints.keySet()) {
            int delta;
//            System.out.printf("Thead %d receiveAndProcessFP1. \n", Thread.currentThread().getId());
            // cluster remove
            if (fingerprints.get(id) < Constants.threadhold) {
//                System.out.printf("Thead %d receiveAndProcessFP2. \n", Thread.currentThread().getId());
                delta = fingerprints.get(id) - Constants.threadhold;

                unitsStatusMap.compute(id, (key, value) -> {
                    value.updateCount(delta);
                    value.belongedDevices.remove(edgeDeviceHashCode);
                    //替换到while flag一开始的地方
//                    if (value.belongedDevices.isEmpty()) {
//                        unitsStatusMap.remove(id);
//                    }
                    return value;
                });

            } else {
                delta = fingerprints.get(id);
                unitsStatusMap.compute(id, (key, value) -> {
                    if (value == null) {
                        value = new UnitInNode(id, 0);
                    }
                    value.updateCount(delta);
                    value.belongedDevices.add(edgeDeviceHashCode);
                    return value;
                });
            }
        }
        ArrayList<Thread> threads = new ArrayList<>();
        count.incrementAndGet();
        boolean flag = count.compareAndSet(this.clientsForDevices.size(), 0);
        if (flag) {
            Iterator<Map.Entry<List<Double>, UnitInNode>> iterator = unitsStatusMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<List<Double>, UnitInNode> entry = iterator.next();
                UnitInNode unitInNode = entry.getValue();
                if (unitInNode.belongedDevices.isEmpty()) {
                    iterator.remove();
                }
            }

            unitResultInfo.clear();
            // node has finished collecting data, entering into the N-N phase, only one thread go into this loop
            this.flag = true; //indicate to other nodes I am ready
            for (UnitInNode unitInNode : unitsStatusMap.values()) {
//                unitInNode.updateSafeness();
                if (unitInNode.pointCnt > maxK) {
                    unitInNode.isSafe = 2;
                } else {
                    unitInNode.isSafe = 1;
                }
            }

            //自己有的clients汇总答案信息
            List<List<Double>> unSafeUnits =
                    unitsStatusMap.keySet().stream().filter(key -> unitsStatusMap.get(key).isSafe != 2).toList();
            for (List<Double> unsafeUnit : unSafeUnits) {
                List<UnitInNode> unitInNodeList = unitsStatusMap.values().stream()
                        .filter(x -> this.handler.neighboringSet(unsafeUnit, x.unitID, maxR)).toList();
                if (!unitResultInfo.containsKey(unsafeUnit)) {
                    unitResultInfo.put(unsafeUnit, Collections.synchronizedList(new ArrayList<>()));
                }
//                System.out.printf("Thead %d: Node is ready4.\n",Thread.currentThread().getId());
                unitInNodeList.forEach(
                        x -> {
                            UnitInNode unitInNode = new UnitInNode(x);
                            unitResultInfo.get(unsafeUnit).add(unitInNode);
                        }
                );
//                System.out.printf("Thead %d: Node is ready5.\n",Thread.currentThread().getId());
            }

            for (int edgeNodeCode : this.clientsForEdgeNodes.keySet()) {
                while (!EdgeNodeNetwork.nodeHashMap.get(edgeNodeCode).handler.flag) {
                    // Waiting for flag to be set
                }
                Thread t = new Thread(() -> {
                    try {
                        Map<List<Double>, List<UnitInNode>> result = this.clientsForEdgeNodes.get(edgeNodeCode).provideNeighborsResult(unSafeUnits, this.belongedNode.hashCode());
                        for (List<Double> unitID : result.keySet()) {
                            List<UnitInNode> unitInNodeList = result.get(unitID);
                            unitResultInfo.compute(unitID, (key, value) -> {
                                if (value == null) {
                                    return Collections.synchronizedList(unitInNodeList);
                                } else {
                                    unitInNodeList.forEach(x -> {
                                        for (UnitInNode unitInNode : value) {
                                            if (unitInNode.unitID.equals(x.unitID)) {
                                                unitInNode.updateCount(x.pointCnt);
                                                unitInNode.belongedDevices.addAll(x.belongedDevices);
                                                return;
                                            }
                                        }
                                        UnitInNode unitInNode = new UnitInNode(x);
                                        value.add(unitInNode);
                                    });
                                    return value;
                                }
                            });
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
                threads.add(t);
                t.start();
            }

            for (Thread t : threads) {
                try {
                    t.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //Pruning Phase
            pruning();
            //send result back to the belonged device;
            sendDeviceResult();
        }
    }

    public void pruning() {
//        System.out.printf("Thead %d: pruning.\n",Thread.currentThread().getId());
        for (List<Double> UnitID : unitResultInfo.keySet()) {
            List<UnitInNode> list = unitResultInfo.get(UnitID);
            //add up all point count
//            Optional<UnitInNode> exist = list.stream().filter(x -> x.unitID.equals(UnitID) && (x.pointCnt > Constants.K)).findAny();
            Optional<UnitInNode> exist = list.stream().filter(x -> x.unitID.equals(UnitID) && (x.pointCnt > maxK)).findAny();
            if (exist.isPresent()) {
                unitsStatusMap.get(UnitID).isSafe = 2;
            }
            int totalCnt = list.stream().mapToInt(x -> x.pointCnt).sum();
//            if (totalCnt <= Constants.K) {
            if (totalCnt <= minK) {
                unitsStatusMap.get(UnitID).isSafe = 0;
            }
        }
    }

    /**
     * @param unSateUnits:  units need to find neighbors in this node
     * @param edgeNodeHash: from which node
     * @description find whether there are neighbor unit in local node
     */
    @Override
    public Map<List<Double>, List<UnitInNode>> provideNeighborsResult(List<List<Double>> unSateUnits, int edgeNodeHash) {
//        System.out.printf("Thead %d provideNeighborsResult. \n", Thread.currentThread().getId());
        Map<List<Double>, List<UnitInNode>> result = new HashMap<>();
        for (List<Double> unit : unSateUnits) {
            List<UnitInNode> unitInNodeList = unitsStatusMap.values().stream()
                    .filter(x -> this.handler.neighboringSet(unit, x.unitID, maxR)).toList();
            result.put(unit, unitInNodeList);
        }
        return result;
    }

    public Set<Vector> result;
    AtomicInteger dataSize = new AtomicInteger(0);
    /*
    public Set<Vector> uploadAndDetectOutlier(List<Vector> data) throws InvalidException, TException {
//        System.out.printf("Thead %d uploadAndDetectOutlier.\n", Thread.currentThread().getId() );
        allData.addAll(data);
        count.incrementAndGet();
        boolean flag = count.compareAndSet(Constants.dn * Constants.nn, 0);
        // wait for all nodes to finish uploading && current slide after first window
        if (flag) {
            if (Constants.currentSlideID == 0||Constants.currentSlideID >= Constants.nS) {
                dataSize = new AtomicInteger(0);
            }
            dataSize.addAndGet(allData.size());
            if (Constants.currentSlideID >= Constants.nS - 1) {
                System.out.println("Each device get data size is " + dataSize);
            }
//            System.out.printf("Thead %d all nodes have finished uploading data.\n", Thread.currentThread().getId() );
            this.detector.detectOutlier(allData);
            if (Constants.currentSlideID >= Constants.nS - 1){
                result = new HashSet<>(this.detector.outlierVector);
            }
            else result = new HashSet<>();
            this.flag = true;
        }
        while (!this.flag){
        }
        allData.clear();
        return result;
    }*/
    public volatile boolean ready = false;

    // for centralized
    @Override
    public Set<Vector> uploadAndDetectOutlier(List<Vector> data) throws InvalidException, TException {
        rawData.addAll(data);
        dataSize.addAndGet(data.size());
        allData.addAll(data);
        count.incrementAndGet();
        boolean localFlag = count.compareAndSet(Constants.dn, 0);
        // wait for all nodes to finish uploading && current slide after first window
        if (localFlag) {
            this.ready = true;
            ArrayList<Thread> threads = new ArrayList<>();
            for (int edgeNodeCode : EdgeNodeNetwork.nodeHashMap.keySet()) {
                if (edgeNodeCode == this.belongedNode.hashCode()) continue;
                Thread thread = new Thread(() -> {
                    try {
                        List<Vector> eachData = clientsForEdgeNodes.get(edgeNodeCode).sendAllNodeData();
                        allData.addAll(eachData);
                        dataSize.addAndGet(eachData.size());
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                });
                thread.start();
                threads.add(thread);
            }
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (Constants.currentSlideID >= Constants.nS - 1) {
//                System.out.println("Each node get data size is " + (dataSize.get()));
                dataSize.set(0);
            }
            this.detector.detectOutlier(allData);
            if (Constants.currentSlideID >= Constants.nS - 1) {
                result = new HashSet<>(this.detector.outlierVector);
            } else result = new HashSet<>();
            this.flag = true; // 同步device
            allData.clear();
            rawData.clear();
        }
        while (!this.flag) {
        }
        return result;
    }

    public volatile boolean ready_RK = false;
    public List<Integer> Ks = Collections.synchronizedList(new ArrayList<>());
    public List<Double> Rs = Collections.synchronizedList(new ArrayList<>());
    volatile Boolean flag_RK = false;

    @Override
    public double synchronizeR_K(int K, double R) {
        // 1.等待所有自己的device上传RK，找到local的 minR minK maxR maxK
        count_RK.incrementAndGet();
        Ks.add(K);
        Rs.add(R);
        boolean localFlag = count_RK.compareAndSet(Constants.dn, 0);
        System.out.println(count_RK);
        System.out.println(localFlag);

        if (localFlag) {
            minR = Collections.min(Rs);
            minK = Collections.min(Ks);
            maxR = Collections.max(Rs);
            maxK = Collections.max(Ks);

            Rs.clear();
            Ks.clear();
            Rs.add(minR);
            Rs.add(maxR);
            Ks.add(minK);
            Ks.add(maxK);

            // 2.调用其他node的sendRK, 找到global的 minR minK maxR maxK
            this.ready_RK = true;
            ArrayList<Thread> threads = new ArrayList<>();
            for (int edgeNodeCode : EdgeNodeNetwork.nodeHashMap.keySet()) {
                if (edgeNodeCode == this.belongedNode.hashCode()) continue;
                Thread thread = new Thread(() -> {
                    try {
                        List<Double> res = clientsForEdgeNodes.get(edgeNodeCode).sendRs();
                        Rs.addAll(res);
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                });
                thread.start();
                threads.add(thread);
            }
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (int edgeNodeCode : EdgeNodeNetwork.nodeHashMap.keySet()) {
                if (edgeNodeCode == this.belongedNode.hashCode()) continue;
                Thread thread = new Thread(() -> {
                    try {
                        List<Integer> res = clientsForEdgeNodes.get(edgeNodeCode).sendKs();
                        Ks.addAll(res);
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                });
                thread.start();
                threads.add(thread);
            }
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            minR = Collections.min(Rs);
            minK = Collections.min(Ks);
            maxR = Collections.max(Rs);
            maxK = Collections.max(Ks);
            System.out.println("node "+ this.hashCode() + ": minR: " + minR + " maxR: " + maxR + " minK: " + minK + " maxK: " + maxK);
            this.flag_RK = true; // 同步device
        }
        while (!this.flag_RK) {
        }
        // 3.储存并返回minR给自己的各device
        return minR;
    }

    public void sendDeviceResult() {
        for (Integer edgeDeviceCode : this.clientsForDevices.keySet()) {
            Thread t = new Thread(() -> {
                List<UnitInNode> list = unitsStatusMap.values().stream().filter(
                        x -> x.belongedDevices.contains(edgeDeviceCode)).toList(); // ���device��ǰ�е�����unit
                HashMap<List<Double>, Integer> status = new HashMap<>();
                for (UnitInNode i : list) {
                    status.put(i.unitID, i.isSafe);
                }
                list = unitsStatusMap.values().stream().filter(
                        x -> (x.belongedDevices.contains(edgeDeviceCode) && (x.isSafe == 1))).toList();
                HashMap<Integer, Set<List<Double>>> result = new HashMap<>();
                //deviceHashCode: unitID
                for (UnitInNode unitInNode : list) {
                    unitResultInfo.get(unitInNode.unitID).forEach(
                            x -> {
                                Set<Integer> deviceList = x.belongedDevices;
                                for (Integer y : deviceList) {
                                    if (!result.containsKey(y)) {
                                        result.put(y, new HashSet<>());
                                    }
                                    result.get(y).add(x.unitID);
                                }
                            }
                    );
                }
                try {
                    this.clientsForDevices.get(edgeDeviceCode).getExternalData(status, result);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
        }
    }

    @Override
    public List<Vector> sendAllNodeData() throws InvalidException, TException {
//        System.out.println("Thead " + Thread.currentThread().getId() + " sendAllNodeData 457");
        while (!this.ready) {
        }
        return new ArrayList<>(this.rawData);
    }

    @Override
    public List<Double> sendRs() throws InvalidException, TException {
        while (!this.ready_RK) {
        }
        List<Double> res = new ArrayList<>();
        res.add(minR);
        res.add(maxR);
        return res;
    }

    @Override
    public List<Integer> sendKs() throws InvalidException, TException {
        while (!this.ready_RK) {
        }
        List<Integer> res = new ArrayList<>();
        res.add(minK);
        res.add(maxK);
        return res;
    }


}
