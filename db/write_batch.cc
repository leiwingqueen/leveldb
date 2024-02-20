// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"

#include "leveldb/db.h"

#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

// scan the rep_ data. check the tag in record and use handler to do the right operation
// use GetLengthPrefixedSlice function to get the key value from Slice
Status WriteBatch::Iterate(Handler* handler) const {
    Slice input(rep_);
    if (input.size() < kHeader) {
        return Status::Corruption("malformed WriteBatch (too small)");
    }
    // iterate implement
    int count = WriteBatchInternal::Count(this);
    input.remove_prefix(kHeader);
    for (int i = 0; i < count; ++i) {
        char tag = input[0];
        input.remove_prefix(1);
        if (tag == kTypeValue) {
            uint32_t keySize;
            GetVarint32(&input, &keySize);
            GetVarint32(&input, &keySize);

            handler->Put(Slice(input.data(),keySize),Slice)
        }else if(tag==kTypeDeletion){

        }else{
            // Error type
        }
    }
    return Status::OK();
}

int WriteBatchInternal::Count(const WriteBatch* b) {
    // see resp_ definition
    return DecodeFixed32(b->rep_.data()+8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
    // see resp_ definition
    char* p = &b->rep_[8];
    EncodeFixed32(p, n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  // get sequence number
  uint64_t num = DecodeFixed64(b->rep_.data());
  return SequenceNumber(num);
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  // set sequence number
    EncodeFixed64(&b->rep_[0],seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  // use PutLengthPrefixedSlice to add key and value to rep_
    this->rep_.push_back(static_cast<char>(kTypeValue));
    PutLengthPrefixedSlice(&this->rep_, key);
    PutLengthPrefixedSlice(&this->rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
    this->rep_.push_back(static_cast<char>(kTypeDeletion));
    PutLengthPrefixedSlice(&this->rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
// - helper of memTable.
// - difference between MemTable and MemTableInserter is that MemTableInserter
// maintained the sequence number
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
