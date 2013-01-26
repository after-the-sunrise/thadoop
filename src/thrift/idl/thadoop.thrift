
namespace java jp.gr.java_conf.afterthesunrise.thadoop.sample

enum ThadoopType {
 FOO
 BAR
 HOGE
}

struct ThadoopStruct {
  1: optional i32           vVersion
  2: optional string        vAuthor
  3: optional ThadoopType   vType
}

struct ThadoopSample {
  1: optional bool          vBoolean
  2: optional byte          vByte
  3: optional i16           vShort
  4: optional i32           vInt
  5: optional i64           vLong
  6: optional double        vDouble
  7: optional string        vString
  8: optional binary        vBinary
 21: optional list<i32>     vList
 22: optional set<i32>      vSet
 23: optional map<i32, i64> vMap
 31: optional ThadoopStruct vStruct
}
