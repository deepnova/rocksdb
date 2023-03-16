#include "db/db_test_util.h"
//#include "port/stack_trace.h"
//#include "rocksdb/iostats_context.h"
//#include "rocksdb/listener.h"
//#include "rocksdb/utilities/debug.h"
//#include "test_util/mock_time_env.h"
#include <gtest/gtest.h>
#include "rocksdb/separator_prefix_sst_partitioner.h"

namespace ROCKSDB_NAMESPACE {

#if !defined(ROCKSDB_LITE)

class SstPartitionTest : public testing::WithParamInterface<bool> {
public:
  SstPartitionTest(){}
  ~SstPartitionTest(){}
};

TEST(SstPartitionTest, First){

  std::string separator = "_";
  auto sst_partitioner_factory = NewSstPartitionerSeparatorPrefixFactory(separator);
  std::unique_ptr<SstPartitioner> partitioner;

  SstPartitioner::Context context;
  context.is_full_compaction = false;
  context.is_manual_compaction = false;
  context.output_level = 6;
  context.smallest_user_key = "aaa";
  context.largest_user_key = "zzz";
  partitioner = sst_partitioner_factory->CreatePartitioner(context);
  ASSERT_TRUE(partitioner != NULL);

  EXPECT_FALSE(partitioner->ShouldPartition(
               PartitionerRequest(Slice("aaa_k1"), Slice("aaa_k2"), 0)
             ));

  EXPECT_TRUE(partitioner->ShouldPartition(
               PartitionerRequest(Slice("aaa_k1"), Slice("aaa_k2"), 268435457)
             ));

  EXPECT_TRUE(partitioner->ShouldPartition(
               PartitionerRequest(Slice("aaa_k1"), Slice("bbb_k2"), 0)
             ));
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

