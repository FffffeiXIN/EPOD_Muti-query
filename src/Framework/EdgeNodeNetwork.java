package Framework;

import RPC.DeviceService;
import RPC.EdgeNodeService;
import RPC.Vector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import test.testNetwork;
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
//    public static BufferedWriter outlierNaiveFw;
//    public static BufferedWriter naiveInfo;

    public static BufferedWriter getDataInfoCSV;
    public static BufferedWriter supportDeviceInfoCSV;

    public static boolean multiQuery = Constants.isMultipleQuery;
    public static AtomicInteger receive_minR_device = new AtomicInteger(0);

    //==================for measurement==================
    static long time = 0;
    static long totalTime = 0;


    static {
        outliers = Collections.synchronizedSet(new HashSet<>());
        try {
//            getDataInfoCSV = new BufferedWriter(new FileWriter(Constants.getDataInfoCSV));
//            supportDeviceInfoCSV = new BufferedWriter(new FileWriter(Constants.supportDeviceInfoCSV));


            outlierFw = new BufferedWriter(new FileWriter(Constants.resultFile));
//            outlierNaiveFw = new BufferedWriter(new FileWriter(Constants.resultNaiveFile));
//            naiveInfo = new BufferedWriter(new FileWriter(Constants.naiveInfo));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static EdgeNode createEdgeNode() {
        EdgeNode edgeNode = new EdgeNode();
        nodeHashMap.put(edgeNode.hashCode(), edgeNode);
        return edgeNode;
    }

    public static void resetEdgeNetwork() {
        time = 0;
        totalTime = 0;
        deviceHashMap = new HashMap<>();
        nodeHashMap = new HashMap<>();
//        dataTransfered = new AtomicLong(0);
//        supportDevices = new AtomicLong(0);
        outliers = Collections.synchronizedSet(new HashSet<>());
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
//        System.out.println("# Dataset: " + Constants.dataset);
//        System.out.println("Method: " + Constants.methodToGenerateFingerprint);
//        System.out.println("Dim: " + Constants.dim);
//        System.out.println("dn/nn: " + Constants.dn + "/" + Constants.nn);
//        System.out.println("R/K/W/S: " + Constants.R + "/" + Constants.K + "/" + Constants.W + "/" + Constants.S);
//        System.out.println("# of windows: " + (Constants.nW));

        //begin when create, avoiding repeated port
//        for (EdgeNode node : nodeHashMap.values()) {
//            node.begin();
//        }
//        for (Device device : deviceHashMap.values()) {
//            device.begin();
//        }

        //set clients for nodes and devices
        for (EdgeNode node : nodeHashMap.values()) {
            setClientsForEdgeNodes(node);
        }
        for (Device device : deviceHashMap.values()) {
            setClientsForDevices(device);
        }

        //========================for multiple query========================
        //synchronize global parameters
        if (multiQuery)
            parametersPreprocessing();

        for (Device device : deviceHashMap.values()) {
            device.handler.setDetectMethod();
        }

        try {
            outlierFw = new BufferedWriter(new FileWriter(Constants.resultFile));
//            outlierNaiveFw = new BufferedWriter(new FileWriter(Constants.resultNaiveFile));
//            naiveInfo = new BufferedWriter(new FileWriter(Constants.naiveInfo));

//            getDataInfoCSV = new BufferedWriter(new FileWriter(Constants.getDataInfoCSV));
//            supportDeviceInfoCSV = new BufferedWriter(new FileWriter(Constants.supportDeviceInfoCSV));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int itr = 0;
        while (itr < Constants.nS + Constants.nW - 1) {

//            dataTransfered.set(0);
            for (EdgeNode node : nodeHashMap.values()) {
                node.handler.flag = false;
                node.handler.ready = false;
            }
            for (Device device : deviceHashMap.values()) {
                device.handler.ready = false;
            }
            ArrayList<Thread> arrayList = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Device device : deviceHashMap.values()) {
                int finalItr = itr;
                Thread t = new Thread(() -> {
                    try {
                        if (Constants.methodToGenerateFingerprint.contains("CENTRALIZE")) {
                            outliers.addAll(device.handler.detectOutlier_Centralize(finalItr));
                        } else if (Constants.methodToGenerateFingerprint.contains("P2P")) {
                            outliers.addAll(device.handler.detectOutlier_P2P(finalItr));
                        } else {
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
            long temp_time = System.currentTimeMillis() - start;
            time += temp_time;
            totalTime += temp_time;
            if (itr >= Constants.nS - 1) {
                //per slide
//                System.out.println("===============================");
//                System.out.println("This is the " + itr + " slides.");

//                getDataInfoCSV.write("\n");
//                supportDeviceInfoCSV.write("\n");
//                getDataInfoCSV.flush();
//                supportDeviceInfoCSV.flush();
//                getDataInfoCSV.write("Data transfer per devices is: " + (int)(dataTransfered.doubleValue() / (Constants.dn * Constants.nn))+"\n");

//                System.out.println("Time cost for this window is : " + time);
//                System.out.println("Average Data transfered is: " + dataTransfered * 1.0 / (Constants.dn * Constants.nn));
//                System.out.println("Data transfered so far is: " + dataTransfered);
//                System.out.println("Interacted clients so far is: " + supportDevices);
//                System.out.println("Total time cost is : " + totalTime);
                time = 0;
            }
            printOutliers();

//            if (itr == Constants.nS + Constants.nS - 1)
//                testing.write("Number of outliers: " + outliers.size()+"\n");
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

//        System.out.println("Average time cost is: " + time * 1.0 / (Constants.nS + Constants.nW - 1)); // todo: 感觉优点问题？
//        testing.write("Method: "+Constants.methodToGenerateFingerprint);
//        testNetwork.testing.write("R = "+Constants.R+"\n");
//        testNetwork.testing.write("K = "+Constants.K+"\n");
//        testNetwork.testing.write("Ratio = "+ Constants.mix_rate_node);
//        if (testNetwork.cnt!=0)
        testNetwork.sum += totalTime * 1.0 / (Constants.nS + Constants.nW - 1);
//        System.out.println("Average time cost per slide is: " + totalTime * 1.0 / (Constants.nS + Constants.nW - 1));
//        testNetwork.testing.write("Average time cost per slide is: " + totalTime * 1.0 / (Constants.nS + Constants.nW - 1)+"\n");
//        System.out.println("Total time cost is: " + totalTime);
//        testNetwork.testing.write("Total time cost is: " + totalTime+"\n");
//        testNetwork.testing.write("Data transfered so far is: " + dataTransfered+"\n");
//        testNetwork.testing.write("===============================\n");
//        testNetwork.testing.flush();
//        System.out.println("Total interacted clients is: " + supportDevices);
        outlierFw.close();
//        outlierNaiveFw.close();
//        naiveInfo.close();
//        getDataInfoCSV.flush();
//        getDataInfoCSV.close();
//        supportDeviceInfoCSV.close();
        stopNetwork();
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
                outlierFw.write(v.backup.values + "\n");
            }
        }
        else {
            List<Vector> list = tmpList.stream().sorted(Comparator.comparing(o -> o.values.get(0))).toList();
            for (Vector v : list) {
                outlierFw.write(v.values + "\n");
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
        resetEdgeNetwork();
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


    public static void parametersPreprocessing() throws TException {
        // 1. 调用divice的上传R K
        ArrayList<Thread> threads = new ArrayList<>();
        for (Device device : deviceHashMap.values()) {
            Thread t = new Thread(() -> {
                try {
                    device.handler.uploadR_K();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (receive_minR_device.get()<Constants.dn*Constants.nn) {
        }
    }
}
