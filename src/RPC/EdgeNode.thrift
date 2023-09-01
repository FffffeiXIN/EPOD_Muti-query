
include "Device.thrift"
namespace java RPC

service EdgeNodeService {
  /**
   * A method definition looks like C code. It has a return type, arguments,
   * and optionally a list of exceptions that it may throw. Note that argument
   * lists and exception lists are specified using the exact same syntax as
   * field lists in struct or exception definitions.
   */
   void receiveAndProcessFP(1: map<list<double>, i32> fingerprints, 2: i32 edgeDeviceHashCode) throws (1: Device.InvalidException invalid),
   map<list<double>, list<Device.UnitInNode>> provideNeighborsResult(1:list<list<double>> unSateUnits, 2: i32 edgeNodeHash) throws (1: Device.InvalidException invalid),
   set<Device.Vector> uploadAndDetectOutlier(1:list<Device.Vector> data) throws (1: Device.InvalidException invalid),
   list<Device.Vector> sendAllNodeData() throws (1: Device.InvalidException invalid),
   double synchronizeR_K(1: i32 K, 2: double R) throws (1: Device.InvalidException invalid),
   list<double> sendRs() throws (1: Device.InvalidException invalid),
   list<i32> sendKs() throws (1: Device.InvalidException invalid),
}