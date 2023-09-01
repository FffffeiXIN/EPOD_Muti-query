package Framework;

import Detector.Detector;
import Detector.MCOD;
import Detector.NewNETS;
import RPC.DeviceService;
import RPC.EdgeNodeService;
import RPC.Vector;
import org.apache.thrift.TException;
import test.testNetwork;
import utils.Constants;
import utils.DataGenerator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unchecked")
public class DeviceImpl implements DeviceService.Iface {

    //=============================For All===============================
    public Device belongedDevice;
    public int deviceId;
    public List<Vector> rawData = new ArrayList<>();
    public DataGenerator dataGenerator;
    public Detector detector;
    volatile public boolean ready = false;

    //=============================EPOD===============================
//    public Map<List<Double>, Integer> fullCellDelta; //fingerprint
    public Map<Integer, DeviceService.Client> clientsForDevices; //And for P2P
    public EdgeNodeService.Client clientsForNearestNode;

    //=============================P2P===============================
    public List<Vector> allData;

    //========================for multiple query========================
    Random random = new Random();
    public double myR;
    public int myK;
    public double minR;

    //==================for measurement==================
    public double processedExternalPoints = 0.0;

    public DeviceImpl(int deviceId, Device belongedDevice) {
        this.belongedDevice = belongedDevice;
        this.deviceId = deviceId;
        this.dataGenerator = new DataGenerator(deviceId);
        this.allData = Collections.synchronizedList(new ArrayList<>());

        //========================for multiple query========================
        if (Constants.isVariousR)
//            this.myR = Constants.R + random.nextDouble() * Constants.R * 0.3 * Math.pow(-1, deviceId);
            this.myR = 0.2 + deviceId * 0.1; // fixed test for STK
        else this.myR = Constants.R;

        if (Constants.isVariousK)
//            this.myK = Constants.K + random.nextInt((int) (Constants.K * 0.3)) * (int) Math.pow(-1, deviceId);
            this.myK = 5 + deviceId; // fixed test for STK
        else this.myK = Constants.K;
        minR = Constants.R;

//        if (Constants.methodToGenerateFingerprint.contains("NETS")) {
//            this.detector = new NewNETS(0);
//        } else if (Constants.methodToGenerateFingerprint.contains("MCOD")) {
//            this.detector = new MCOD();
//        }
        System.out.println("device "+ deviceId + " myR is " + myR + " myK is " + myK);
    }

    public void setDetectMethod(){
        if (Constants.methodToGenerateFingerprint.contains("NETS")) {
            this.detector = new NewNETS(0, this.myK, this.minR, this.myR);
        } else if (Constants.methodToGenerateFingerprint.contains("MCOD")) {
            this.detector = new MCOD(this.myK, this.minR, this.myR);
        }
    }

    public void setClients(EdgeNodeService.Client clientsForNearestNode, Map<Integer, DeviceService.Client> clientsForDevices) {
        this.clientsForDevices = clientsForDevices;
        this.clientsForNearestNode = clientsForNearestNode;
    }

    public void getRawData(int itr) {
        Date currentRealTime = new Date();
        // 1000ms  10
        currentRealTime.setTime(dataGenerator.firstTimeStamp.getTime() + (long) 1000 * itr);
        this.rawData = dataGenerator.getTimeBasedIncomingData(currentRealTime, 1);
    }

    public Set<? extends Vector> detectOutlier(int itr) throws Throwable {
        processedExternalPoints = 0.0;
        this.ready = false;
        //get initial data
        Constants.currentSlideID = itr;
        getRawData(itr);

        //step1
        if (itr > Constants.nS - 1) {
            this.detector.clearFingerprints();
        }
        this.detector.detectOutlier(this.rawData);

        //step2
        if (itr >= Constants.nS - 1) {
            this.clientsForNearestNode.receiveAndProcessFP(this.detector.fullCellDelta, this.belongedDevice.hashCode());
        } else return new HashSet<>();

        //Wait until all external data is got
        while (!this.ready) {
        }
        this.detector.processOutliers();
//        System.out.printf("Thead %d finished. \n", Thread.currentThread().getId());
        return this.detector.outlierVector;
    }


    public Map<List<Double>, List<Vector>> sendData(Set<List<Double>> bucketIds, int deviceHashCode) {
        return this.detector.sendData(bucketIds, deviceHashCode);
    }
    AtomicInteger dataSize = new AtomicInteger(0);
    public void getExternalData(Map<List<Double>, Integer> status, Map<Integer, Set<List<Double>>> result) {
//        System.out.printf("Thead %d getExternalData. \n", Thread.currentThread().getId());
        this.detector.status = status;
        ArrayList<Thread> threads = new ArrayList<>();
//        try {
//        System.out.println("Device "+this.belongedDevice.hashCode() + " support device size is " + result.keySet().stream().filter(x -> x != this.belongedDevice.hashCode()).toList().size()+"\n");
//            EdgeNodeNetwork.supportDeviceInfo.flush();
//            EdgeNodeNetwork.supportDeviceInfoCSV.write(result.keySet().stream().filter(x -> x != this.belongedDevice.hashCode()).toList().size()+",");
//            EdgeNodeNetwork.supportDeviceInfoCSV.flush();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        testNetwork.supportDevices.addAndGet(result.keySet().stream().filter(x -> x != this.belongedDevice.hashCode()).toList().size());
        for (Integer deviceCode : result.keySet()) {
            if (deviceCode.equals(this.belongedDevice.hashCode())) continue;
            //HashMap<Integer,HashSet<ArrayList<?>>>
            Thread t = new Thread(() -> {
                try {
                    Map<List<Double>, List<Vector>> data = this.clientsForDevices.get(deviceCode).sendData(result.get(deviceCode), this.belongedDevice.hashCode());
                    if (!this.detector.externalData.containsKey(Constants.currentSlideID)) {
                        this.detector.externalData.put(Constants.currentSlideID, Collections.synchronizedMap(new HashMap<>()));
                    }
                    data.values().forEach(x -> dataSize.addAndGet(x.size()));
                    Map<List<Double>, List<Vector>> map = this.detector.externalData.get(Constants.currentSlideID);
                    data.keySet().forEach(
                            x -> {
                                if (x.get(0) == 434.0&& Constants.currentSlideID == 20){
                                    int a =1;
                                }
                                if (!map.containsKey(x)) {
                                    map.put(x, Collections.synchronizedList(new ArrayList<>()));
                                }
                                map.get(x).addAll(data.get(x));
                            }
                    );
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
        if (Constants.currentSlideID >= Constants.nS - 1) {
//            try {
//                EdgeNodeNetwork.getDataInfoCSV.write(dataSize+",");
//                EdgeNodeNetwork.getDataInfoCSV.flush();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
            testNetwork.dataTransfered.addAndGet(dataSize.get());
        }
        dataSize.set(0);
        this.ready = true;
    }

    public Set<? extends Vector> detectOutlier_P2P(int itr) throws Throwable {
        //step1: get data
        Constants.currentSlideID = itr;
        getRawData(itr);
        this.ready = true;
        allData.clear();
        allData.addAll(rawData);
        //step2: ask other device for data
        ArrayList<Thread> threads = new ArrayList<>();
        for (DeviceService.Client client : clientsForDevices.values()) {
            Thread t = new Thread(() -> {
                try {
                    List<Vector> data = client.sendAllLocalData();
                    dataSize.addAndGet(data.size());
                    allData.addAll(data);
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
        if (Constants.currentSlideID >= Constants.nS - 1) {
//            System.out.println("Each device get data size is " + dataSize);
            dataSize.set(0);
        }
        this.detector.detectOutlier(allData);
        if (itr >= Constants.nS - 1) {
            return this.detector.outlierVector;
        } else return new HashSet<>();
    }

    public Set<? extends Vector> detectOutlier_Centralize(int itr) throws Throwable {
        Constants.currentSlideID = itr;
        getRawData(itr);
        Set<? extends Vector> result = clientsForNearestNode.uploadAndDetectOutlier(this.rawData);
        return result;
    }

    @Override
    public List<Vector> sendAllLocalData() {
        while (!this.ready){
        }
        return this.rawData;
    }

    //==============for multi-query================
    public void uploadR_K() throws TException {
        // 调用node端的synchronizeRK()
        this.minR = this.clientsForNearestNode.synchronizeR_K(this.myK, this.myR);
//        setDetectMethod();
        EdgeNodeNetwork.receive_minR_device.getAndIncrement();
        System.out.println("device "+ deviceId + " myR is " + myR + " myK is " + myK + " minR is " + minR);
    }
}
