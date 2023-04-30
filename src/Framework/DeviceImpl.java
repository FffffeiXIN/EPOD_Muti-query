package Framework;

import Detector.Detector;
import Detector.MCOD;
import Detector.NewNETS;
import RPC.DeviceService;
import RPC.EdgeNodeService;
import RPC.Vector;
import utils.Constants;
import utils.DataGenerator;

import java.util.*;

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
    public HashMap<Integer, Integer> historyRecord; //??????????device????��????????????deviceID->slideID
    public Map<Integer, DeviceService.Client> clientsForDevices; //And for P2P
    public EdgeNodeService.Client clientsForNearestNode;

    //=============================P2P===============================
    public List<Vector> allData;

    public DeviceImpl(int deviceId, Device belongedDevice) {
        this.belongedDevice = belongedDevice;
        this.deviceId = deviceId;
        this.dataGenerator = new DataGenerator(deviceId);
        this.allData = Collections.synchronizedList(new ArrayList<>());
        if (Constants.methodToGenerateFingerprint.contains("NETS")) {
            this.detector = new NewNETS(0);
        } else if (Constants.methodToGenerateFingerprint.contains("MCOD")) {
            this.detector = new MCOD();
        }
        this.historyRecord = new HashMap<>();
        for (int deviceHashCode : EdgeNodeNetwork.deviceHashMap.keySet()) {
            this.historyRecord.put(deviceHashCode, 0);
        }
    }

    public void setClients(EdgeNodeService.Client clientsForNearestNode, Map<Integer, DeviceService.Client> clientsForDevices) {
        this.clientsForDevices = clientsForDevices;
        this.clientsForNearestNode = clientsForNearestNode;
    }

    public void getRawData(int itr) {
        Date currentRealTime = new Date();
        currentRealTime.setTime(dataGenerator.firstTimeStamp.getTime() + (long) Constants.S * 10 * 1000 * itr);
        this.rawData = dataGenerator.getTimeBasedIncomingData(currentRealTime, Constants.S * 10);
    }

    public Set<? extends Vector> detectOutlier(int itr) throws Throwable {
        System.out.println("Thread " + Thread.currentThread().getId() + " detectOutlier");
        this.ready = false;
        //get initial data
        Constants.currentSlideID = itr;
        getRawData(itr);

        //step1: ??????? + ?????????outliers
        if (itr > Constants.nS - 1) {
            this.detector.clearFingerprints();
        }
        this.detector.detectOutlier(this.rawData);

        //step2: ??????
        if (itr >= Constants.nS - 1) {
            this.clientsForNearestNode.receiveAndProcessFP(this.detector.fullCellDelta, this.belongedDevice.hashCode());
        } else return new HashSet<>();

        //?????????? + ????outliers
        while (!this.ready) {
        }
        this.detector.processOutliers();
        System.out.printf("Thead %d finished. \n", Thread.currentThread().getId());
        return this.detector.outlierVector;
    }


    public Map<List<Double>, List<Vector>> sendData(Set<List<Double>> bucketIds, int deviceHashCode) {
        System.out.printf("Thead %d sendData. \n", Thread.currentThread().getId());
        //????????????????????
        int lastSent = Math.max(this.historyRecord.get(deviceHashCode), Constants.currentSlideID - Constants.nS);
        this.historyRecord.put(deviceHashCode, Constants.currentSlideID);
        return this.detector.sendData(bucketIds, lastSent);
    }

    public void getExternalData(Map<List<Double>, Integer> status, Map<Integer, Set<List<Double>>> result) {
        System.out.printf("Thead %d getExternalData. \n", Thread.currentThread().getId());
        this.detector.status = status; //?????��?outliers?????????????????processOutliers()??
        ArrayList<Thread> threads = new ArrayList<>();
        for (Integer deviceCode : result.keySet()) {
            if (deviceCode == this.belongedDevice.hashCode()) continue;
            //HashMap<Integer,HashSet<ArrayList<?>>>
            Thread t = new Thread(() -> {
                try {
                    Map<List<Double>, List<Vector>> data = this.clientsForDevices.get(deviceCode).sendData(result.get(deviceCode), this.belongedDevice.hashCode());
                    if (!this.detector.externalData.containsKey(Constants.currentSlideID)) {
                        this.detector.externalData.put(Constants.currentSlideID, Collections.synchronizedMap(new HashMap<>()));
                    }
                    Map<List<Double>, List<Vector>> map = this.detector.externalData.get(Constants.currentSlideID);//TODO: Check ??????????
                    data.keySet().forEach(
                            x -> {
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
        this.ready = true;
    }

    public Set<? extends Vector> detectOutlier_P2P(int itr) throws Throwable {
        System.out.println("Thead " + Thread.currentThread().getId() + " detectOutlier_P2P");
        this.ready = false;
        Constants.currentSlideID = itr;
        getRawData(itr);
        //step1: ????data
        allData.clear(); //allData ?????????? ?????????NETS ?? MCOD????? @shimin
        allData.addAll(rawData);

        //step2: ???????device??data
        ArrayList<Thread> threads = new ArrayList<>();
        for (DeviceService.Client client : clientsForDevices.values()) {
            Thread t = new Thread(() -> {
                try {
                    List<Vector> data = client.sendAllLocalData();
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
        this.ready = true;

        //step3: detectOutlier
        while (!this.ready) {
        }
        if (itr >= Constants.nS - 1) {
            this.detector.detectOutlier(allData);
        }
        return this.detector.outlierVector;
    }

    public Set<? extends Vector> detectOutlier_Centralize(int itr) throws Throwable {
        System.out.println("Thead " + Thread.currentThread().getId() + " detectOutlier_Centralize");
        Constants.currentSlideID = itr;
        getRawData(itr);
        return clientsForNearestNode.uploadAndDetectOutlier(this.rawData);
    }

    //????slide???
    @Override
    public List<Vector> sendAllLocalData() {
        System.out.printf("Thead %d sendAllLocalData. \n", Thread.currentThread().getId());
        return this.rawData;
    }
}
