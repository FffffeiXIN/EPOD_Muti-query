package Framework;

import RPC.DeviceService;
import RPC.EdgeNodeService;
import RPC.Vector;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import utils.CompareResult;
import utils.Constants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//Measurement:
// latency
// overall data transferred
// # support devices
// (# of processed points) / (# of outliers)

public class EdgeNodeNetwork {
    public static HashMap<Integer, Device> deviceHashMap = new HashMap<>();
    public static HashMap<Integer, EdgeNode> nodeHashMap = new HashMap<>();
    public static Set<Vector> outliers; //only used to print out outlier
    public static BufferedWriter outlierFw;
    public static BufferedWriter outlierNaiveFw;
    public static BufferedWriter naiveInfo;

    //==================for measurement==================
    public static AtomicInteger dataTransfered = new AtomicInteger(0);
    public static AtomicInteger supportDevices = new AtomicInteger(0);
    static long time = 0;
    static long totalTime = 0;


    static {
        outliers = Collections.synchronizedSet(new HashSet<>());
        try {
            outlierFw = new BufferedWriter(new FileWriter(
                    "src\\Result\\" +
                            "_Result_"+Constants.methodToGenerateFingerprint+"_"+ Constants.dataset + "_outliers.txt"));
//            outlierFw = new BufferedWriter(new FileWriter(
//                    "src/Result/" +
//                            "_Result_"+Constants.methodToGenerateFingerprint+"_"+ Constants.dataset + "_outliers.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            outlierNaiveFw = new BufferedWriter(new FileWriter(
                    "src\\Result\\"+
                            "_Result_Naive_" + Constants.dataset + "_outliers.txt"));
//            outlierNaiveFw = new BufferedWriter(new FileWriter(
//                    "src/Result/"+
//                            "_Result_Naive_" + Constants.dataset + "_outliers.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            naiveInfo = new BufferedWriter(new FileWriter(
                    "src\\Result\\"+
                            "_Result_Naive_info" + Constants.dataset + "_outliers.txt"));
//            naiveInfo = new BufferedWriter(new FileWriter(
//                    "src/Result/"+
//                            "_Result_Naive_info" + Constants.dataset + "_outliers.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static EdgeNode createEdgeNode() {
        EdgeNode edgeNode = new EdgeNode();
        nodeHashMap.put(edgeNode.hashCode(), edgeNode);
        return edgeNode;
    }

    public static void createNetwork(int nn, int dn, DeviceFactory edgeDeviceFactory) throws Throwable {
        for (int i = 0; i < nn; i++) {
            EdgeNode node = createEdgeNode();
            ArrayList<Integer> devicesCodes = new ArrayList<>();
            for (int j = 0; j < dn; j++) {
                Device device = edgeDeviceFactory.createEdgeDevice(i * dn + j);
                deviceHashMap.put(device.hashCode(), device);
                device.setNearestNode(node.hashCode());
                devicesCodes.add(device.hashCode());
            }
            node.setDevices(devicesCodes);
        }
    }

    public static void startNetwork() throws Throwable {
        //Print Logs
        System.out.println("# Dataset: " + Constants.dataset);
        System.out.println("Method: " + Constants.methodToGenerateFingerprint);
        System.out.println("Dim: " + Constants.dim);
        System.out.println("dn/nn: " + Constants.dn + "/" + Constants.nn);
        System.out.println("R/K/W/S: " + Constants.R + "/" + Constants.K + "/" + Constants.W + "/" + Constants.S);
        System.out.println("# of windows: " + (Constants.nW));
        for (EdgeNode node : nodeHashMap.values()) {
            node.begin();
        }
        for (Device device : deviceHashMap.values()) {
            device.begin();
        }

        //set clients for nodes and devices
        for (EdgeNode node : nodeHashMap.values()) {
            setClientsForEdgeNodes(node);
        }
        for (Device device : deviceHashMap.values()) {
            setClientsForDevices(device);
        }

        //========================for multiple query========================
        //synchronize global parameters
        parametersPreprocessing(Constants.isMultipleQuery);


        int itr = 0;
        while (itr < Constants.nS + Constants.nW - 1) {
            //per slide
            System.out.println("===============================");
            System.out.println("This is the " + itr + " slides.");
//            dataTransfered.set(0);
            for (EdgeNode node : nodeHashMap.values()) {
                node.handler.flag = false;
                node.handler.ready = false;
            }
            for (Device device : deviceHashMap.values()) {
                device.handler.ready = false;
            }
            if (itr >= Constants.nS - 1) {
                System.out.println("Window " + (itr - Constants.nS + 1));
            }
            ArrayList<Thread> arrayList = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Device device : deviceHashMap.values()) {
                int finalItr = itr;
                Thread t = new Thread(() -> {
                    try {
                        if (Constants.methodToGenerateFingerprint.contains("CENTRALIZE")){
                            outliers.addAll(device.handler.detectOutlier_Centralize(finalItr));
                        }else if (Constants.methodToGenerateFingerprint.contains("P2P")){
                            outliers.addAll(device.handler.detectOutlier_P2P(finalItr));
                        }else {
                            outliers.addAll(device.handler.detectOutlier(finalItr));
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                });
                t.start();
                arrayList.add(t);
            }
            for (Thread t : arrayList) {
                t.join();
            }
            time += System.currentTimeMillis() - start;
            totalTime += time;
            if (itr >= Constants.nS - 1) {
                System.out.println("Time cost for this window is : " + time);
//                System.out.println("Average Data transfered is: " + dataTransfered * 1.0 / (Constants.dn * Constants.nn));
                System.out.println("Data transfered so far is: " + dataTransfered);
                System.out.println("interacted clients so far is: " + supportDevices);
                System.out.println("Total time cost is : " + totalTime);
                time = 0;
            }
            printOutliers();
            outliers.clear();

            //========================== NAIVE ================================
            List<Vector> allData = new ArrayList<>();
            for (Device device : deviceHashMap.values()) {
                allData.addAll(device.handler.rawData);
            }
//            HashSet<Vector> outliers = CompareResult.detectOutliersNaive(allData, itr);
//            //"TAO" "GAS" "STK" "GAU" "EM" "HPC"
//            if (Constants.methodToGenerateFingerprint.equals("NETS")) {
//                List<Vector> list = outliers.stream().sorted(
//                        Vector::compareTo).toList();
//                for (Vector v : list) {
//                    outlierNaiveFw.write(v + "\n");
//                }
//            } else {
//                List<Vector> list = outliers.stream().sorted(
//                        Comparator.comparingInt(o -> o.arrivalTime)).toList();
//                for (Vector v : list) {
//                    outlierNaiveFw.write(v + "\n");
//                }
//            }
//            outlierNaiveFw.write("====================================\n");
//            outlierNaiveFw.flush();
            itr++;
        }
        stopNetwork();
//        System.out.println("Average time cost is: " + time * 1.0 / (Constants.nS + Constants.nW - 1)); // todo: 感觉优点问题？
        System.out.println("Average time cost per slide is: " + totalTime * 1.0 / (Constants.nS + Constants.nW - 1));
        System.out.println("Total time cost is: " + totalTime);
        System.out.println("Total interacted clients is: " + supportDevices);
        outlierFw.close();
        outlierNaiveFw.close();
        naiveInfo.close();
    }

    public static void printOutliers() throws IOException {
        HashSet<Vector> tmpList = new HashSet<>();
        for (Vector v : outliers) {
            Vector tmp = new Vector(v);
            tmp.backup = v.backup;
            tmpList.add(tmp);
        }

        if (Constants.methodToGenerateFingerprint.equals("NETS")) {
            List<Vector> list = tmpList.stream().sorted(Comparator.comparing(o -> o.backup)).toList();
            for (Vector v : list) {
                outlierFw.write(v.backup + "\n");
            }
        }
        else {
            List<Vector> list = tmpList.stream().sorted(Comparator.comparing(o -> o.arrivalTime)).toList();
            for (Vector v : list) {
                outlierFw.write(v + "\n");
            }
        }
        outlierFw.write("====================================\n");
        outlierFw.flush();
    }
    public static void stopNetwork() {
        for (EdgeNode node : nodeHashMap.values()) {
            node.stop();
        }
        for (Device device : deviceHashMap.values()) {
            device.stop();
        }
        System.out.println("Ended!");
    }

    public static void setClientsForEdgeNodes(EdgeNode node) throws TTransportException {
        Map<Integer, EdgeNodeService.Client> clientsForEdgeNodes = new HashMap<>();
        Map<Integer, DeviceService.Client> clientsForDevices = new HashMap<>();

        for (Integer nodeHashCode : nodeHashMap.keySet()) {
            if (nodeHashCode.equals(node.hashCode())) continue;
            TTransport transport = new TSocket("127.0.0.1", nodeHashMap.get(nodeHashCode).port);
            transport.open();
            node.transports.add(transport);
            TProtocol protocol = new TBinaryProtocol(transport);
            EdgeNodeService.Client client = new EdgeNodeService.Client(protocol);
            clientsForEdgeNodes.put(nodeHashCode, client);
        }

        for (Integer deviceCode : node.devicesCodes) {
            TTransport transport = new TSocket("127.0.0.1", deviceHashMap.get(deviceCode).port);
            transport.open();
            node.transports.add(transport);
            TProtocol protocol = new TBinaryProtocol(transport);
            DeviceService.Client client = new DeviceService.Client(protocol);
            clientsForDevices.put(deviceCode, client);
        }
        node.handler.setClients(clientsForEdgeNodes, clientsForDevices);
    }


    public static void setClientsForDevices(Device device) throws TTransportException {
        TTransport transport = new TSocket("127.0.0.1", nodeHashMap.get(device.nearestNodeCode).port);
        transport.open();
        device.transports.add(transport);
        TProtocol protocol = new TBinaryProtocol(transport);
        EdgeNodeService.Client clientForNearestNode = new EdgeNodeService.Client(protocol);

        Map<Integer, DeviceService.Client> clientsForDevices = new HashMap<>();
        for (Integer deviceHashCode : deviceHashMap.keySet()) {
            if (deviceHashCode.equals(device.hashCode())) continue;
            TTransport transport1 = new TSocket("127.0.0.1", deviceHashMap.get(deviceHashCode).port);
            transport1.open();
            device.transports.add(transport1);
            TProtocol protocol1 = new TBinaryProtocol(transport1);
            DeviceService.Client client = new DeviceService.Client(protocol1);
            clientsForDevices.put(deviceHashCode, client);
        }
        device.handler.setClients(clientForNearestNode, clientsForDevices);
    }

    public static void parametersPreprocessing(Boolean isMultipleQuery){
        if (!isMultipleQuery) {
            // 所有设备的R K minR minK maxR maxK 为Constant.R K
        }
        else {
            // 1. 调用divice的上传R K
        }
    }
}
