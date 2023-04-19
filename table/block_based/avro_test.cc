#include "db/db_test_util.h"
#include <gtest/gtest.h>

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/Stream.hh>

//using namespace avro;

namespace ROCKSDB_NAMESPACE {

const char schema_json[] = "{\"type\": \"record\","
                                   "\"name\":\"ComplexInteger\","
                                   "\"fields\": ["
                                     "{\"name\":\"f1\", \"type\":\"string\"},"
                                     "{\"name\":\"f2\", \"type\":\"long\"},"
                                     "{\"name\":\"f3\", \"type\":\"boolean\"}"
                                  "]}";
const int DEFAULT_COUNT = 100;

#if !defined(ROCKSDB_LITE)

class AvroTest : public testing::WithParamInterface<bool> {
public:
  AvroTest(){}
  ~AvroTest(){}

  static void TestEncodeGeneric(const avro::ValidSchema &schemaObj, 
                                unsigned char **buf,
                                size_t &buf_len,
                                size_t &records_count) {

    avro::OutputStreamPtr os = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*os);

    std::string jschema = schemaObj.toJson();
    std::cout << "schema json: " << jschema << std::endl;

    int count = DEFAULT_COUNT;
    avro::GenericRecord r = avro::GenericRecord(schemaObj.root());

    std::cout << "field count: " << r.fieldCount() << std::endl;

    std::string f1;
    for (int i = 0; i < count; ++i) {
        f1 = "f1String_" + std::to_string(i);
        r.field("f1") = f1;
        r.field("f2") = i;
        r.field("f3") = (i%3 == 0) ? true : false;

        size_t c = r.schema()->leaves();
        for (size_t j = 0; j < c; ++j) {
            avro::encode(*encoder, r.fieldAt(j));
        }
    }
    encoder->flush();
    memset(*buf, 0, buf_len);
    buf_len = os->byteCount();
    records_count = count;
    ASSERT_TRUE(buf_len > 0);

    uint8_t *p = nullptr;
    size_t n = 0;
    os->next(&p, &n);
    p = p - buf_len;
    std::cout << "buffer size: " << buf_len << std::endl;
    memcpy(*buf, p, buf_len);
    std::cout << "buf: " << *buf << std::endl;

    //ASSERT_TRUE(buf[0] == p[0]);
    //ASSERT_TRUE(buf[buf_len - 1] == p[buf_len - 1]);
  }

  static void TestDecodeGeneric(const avro::ValidSchema &schemaObj,
                                const unsigned char *buf,
                                const size_t buf_len,
                                const size_t records_count) {

    avro::InputStreamPtr is = avro::memoryInputStream(buf, buf_len);
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*is);

    avro::GenericRecord r = avro::GenericRecord(schemaObj.root());

    for (size_t i = 0; i < records_count; ++i) {
        size_t c = r.schema()->leaves();
        for (size_t j = 0; j < c; ++j) {
            avro::decode<avro::GenericDatum>(*decoder, r.fieldAt(j));
        }

        ASSERT_TRUE(r.field("f1").type() == avro::AVRO_STRING);
        ASSERT_TRUE(r.field("f2").type() == avro::AVRO_LONG);
        ASSERT_TRUE(r.field("f3").type() == avro::AVRO_BOOL);

        std::cout << i << ": f1=" << r.field("f1").value<std::string>() 
                       << ", f2=" << r.field("f2").value<long>()
                       << ", f3=" << r.field("f3").value<bool>()
                       << std::endl;
    }
  }
};

TEST(AvroTest, EncodeAndDecodeGeneric){

  avro::ValidSchema schemaObj = avro::compileJsonSchemaFromString(schema_json);

  unsigned char* buf = new unsigned char[102400];
  size_t buf_len = 102400;
  size_t records_count = 0;

  AvroTest::TestEncodeGeneric(schemaObj, &buf, buf_len, records_count);
  AvroTest::TestDecodeGeneric(schemaObj, buf, buf_len, records_count);

  //delete[] buf;
}

#endif  // !defined(ROCKSDB_LITE)

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  (void)argc;
  (void)argv;
  return 0;
#endif
}


