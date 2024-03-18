//
// Created by leiwingqueen on 2024/3/12.
//
#include "leveldb/db.h"

#include "gtest/gtest.h"
#include "iostream"

namespace leveldb {

class SimpleDBTest : public testing::Test {
 protected:
  void SetUp() override {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db_);
    if (!status.ok()) {
      std::cout << "open db error:" << status.ToString() << std::endl;
    }
    assert(status.ok());
  }
  void TearDown() override {
    delete db_;
  }
  DB* db_;
};

TEST_F(SimpleDBTest, t1) {
  WriteOptions write_options;
  write_options.sync = true;
  Status s = db_->Put(write_options, "key1", "value1");
  ASSERT_TRUE(s.ok());
  std::string value;
  ReadOptions read_options;
  s = db_->Get(read_options, "key1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "value1");
}

TEST_F(SimpleDBTest, t2) {
  WriteOptions write_options;
  write_options.sync = false;
  for (int i = 0; i < 1000; ++i) {
    Status s = db_->Put(write_options, "key" + std::to_string(i),
                        "value" + std::to_string(i));
    ASSERT_TRUE(s.ok());
  }
  std::string value;
  ReadOptions read_options;
  Status s = db_->Get(read_options, "key1", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "value1");
}

}  // namespace leveldb
