
// Usage : ${THRIFT_EXEC} --gen java:beans,hashcode,private-members -out ${THRIFT_OUT_DIR} thadoop.thrift

namespace java jp.gr.java_conf.afterthesunrise.thadoop.sample

struct ThadoopSample {
 1: optional bool   fieldBoolean
 2: optional byte   fieldByte
 3: optional i16    fieldShort
 4: optional i32    fieldInt
 5: optional i64    fieldLong
 6: optional double fieldDouble
 7: optional string fieldString
}

enum ThadoopType {
 FOO
 BAR
 HOGE
}

struct ThadoopSampleCollection {
 1: optional list<ThadoopSample> fieldList
 2: optional set<ThadoopType>    fieldSet
 3: optional map<string, binary> fieldMap
}
