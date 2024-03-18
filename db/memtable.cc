// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include "db/dbformat.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

static void printSlice(Slice s) {
  for (int i = 0; i < s.size(); ++i) {
    std::fprintf(stdout, "0x%d ", s.data()[i]);
  }
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

// hint:
// - user MemTable.table_(SkipList) to save the kv data
// - key size and value size use varint32 save
// - the length of varint32 calculated by the function VarintLength
// - use Arena::Allocate to allocate memory
// - use EncodeVarint32 to write varint32 into buffer
// - use memcpy to write data from key/value data to
// - reference https://zhuanlan.zhihu.com/p/272468157

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]

  // MemTable implement
  // internal data = key_data|sequence(7 byte)|type(1 byte)
  int internal_key_size = key.size() + 8;

  int total_size = VarintLength(internal_key_size) + internal_key_size +
                   VarintLength(value.size()) + value.size();
  char* buffer = arena_.Allocate(total_size);
  // point to the head
  char* p = buffer;
  p = EncodeVarint32(p, internal_key_size);
  memcpy(p, key.data(), key.size());
  p += key.size();
  uint64_t tag = (s << 8) | (type & 0xff);
  EncodeFixed64(p, tag);
  p += 8;
  p = EncodeVarint32(p, value.size());
  memcpy(p, value.data(), value.size());
  table_.Insert(buffer);
  // print result
  // std::fprintf(stdout, "memtable add[finish]...");
  // printSlice(key);
  // std::fprintf(stdout, "\n");
}

// hint:
// - use Table::Iterator scan skip list to find the item
// - use memtable_key to find the first match item. why? you can find the answer
// in InternalKeyComparator::Compare
// - use GetVarint32Ptr to get the size of key length
// - use comparator_ to compare lookup
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  // entry format is:
  //    klength  varint32
  //    userkey  char[klength]
  //    tag      uint64
  //    vlength  varint32
  //    value    char[vlength]
  // Check that it belongs to same user key.  We do not check the
  // sequence number since the Seek() call above should have skipped
  // all entries with overly large sequence numbers.

  // MemTable implement
  Table::Iterator iterator(&table_);
  Slice memtable_key = key.memtable_key();
  iterator.Seek(memtable_key.data());
  if (!iterator.Valid()) {
    return false;
  }
  const char* item = iterator.key();
  uint32_t key_size;
  const char* p = GetVarint32Ptr(item, item + 5, &key_size);
  Slice keySlice1(p, key_size - 8);
  Slice keySlice2(key.user_key());
  if (comparator_.comparator.user_comparator()->Compare(keySlice1, keySlice2) !=
      0) {
    return false;
  }
  // get type
  uint64_t tag = DecodeFixed64(p + key_size - 8);
  ValueType type = static_cast<ValueType>(tag & 0xff);
  if (ValueType::kTypeValue == type) {
    // get value from item
    p += key_size;
    Slice valueSlice = GetLengthPrefixedSlice(p);
    value->assign(valueSlice.data(), valueSlice.size());
    *s = Status::OK();
  } else {
    // deletion
    *s = Status::NotFound("not found");
  }
  /*  std::fprintf(stdout, "memtable get[finish]...");
    printSlice(key.user_key());
    std::fprintf(stdout, "\n");*/
  return true;
}

}  // namespace leveldb
