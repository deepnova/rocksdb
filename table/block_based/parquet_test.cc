#include "db/db_test_util.h"
#include <gtest/gtest.h>

namespace ROCKSDB_NAMESPACE {


#if !defined(ROCKSDB_LITE)

class ParquetTest : public testing::WithParamInterface<bool> {
public:
  ParquetTest(){}
  ~ParquetTest(){}

  // parquet sample code
  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields;
    // Create a primitive node named 'boolean_field' with type:BOOLEAN,
    // repetition:REQUIRED
    fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                         parquet::Type::BOOLEAN, ConvertedType::NONE));
  
    // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
    // logical type:TIME_MILLIS
    fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, parquet::Type::INT32,
                                         ConvertedType::TIME_MILLIS));
  
    // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
    fields.push_back(PrimitiveNode::Make("int64_field", Repetition::REPEATED, parquet::Type::INT64,
                                         ConvertedType::NONE));
  
    fields.push_back(PrimitiveNode::Make("int96_field", Repetition::REQUIRED, parquet::Type::INT96,
                                         ConvertedType::NONE));
  
    fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, parquet::Type::FLOAT,
                                         ConvertedType::NONE));
  
    fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED, parquet::Type::DOUBLE,
                                         ConvertedType::NONE));
  
    // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
    fields.push_back(PrimitiveNode::Make("ba_field", Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
                                         ConvertedType::NONE));
  
    // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
    // repetition:REQUIRED, field_length = FIXED_LENGTH
    fields.push_back(PrimitiveNode::Make("flba_field", Repetition::REQUIRED,
                                         parquet::Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
                                         FIXED_LENGTH));
  
    // Create a GroupNode named 'schema' using the primitive nodes defined above
    // This GroupNode is the root node of the schema tree
    return std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }
};

TEST(ParquetTest, ParquetSchema){
  ASSERT_TRUE(true);
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



