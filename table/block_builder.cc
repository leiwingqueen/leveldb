// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"

#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  // implement BlockBuilder::Reset
  buffer_.clear();
  last_key_.clear();
  counter_ = 0;
  restarts_.clear();
  restarts_.push_back(0);
  finished_ = false;
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // implement BlockBuilder::Finish
  // hint: append restart array to buffer_, then append restarts_.size() to
  // buffer_ each restart is a uint32_t, so you can use PutFixed32 to append
  // each restart return a Slice of buffer_
  // - remember change finished_ to true

  // Append restart array
  // The trailer of the block has the form:
  //     restarts: uint32[num_restarts]
  //     num_restarts: uint32
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  // implement BlockBuilder::Add
  // hint:
  // An entry for a particular key-value pair has the form:
  //     shared_bytes: varint32
  //     unshared_bytes: varint32
  //     value_length: varint32
  //     key_delta: char[unshared_bytes]
  //     value: char[value_length]
  // shared_bytes == 0 for restart points.
  // remember to update last_key_ and counter_
  if (counter_ >= options_->block_restart_interval) {
    last_key_.clear();
    counter_ = 0;
    restarts_.push_back(buffer_.size());
  }
  size_t shared = 0;
  int size = std::min(last_key_piece.size(), key.size());
  while (shared < size && last_key_piece[shared] == key[shared]) {
    shared++;
  }
  size_t unshared = key.size() - shared;
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, unshared);
  PutVarint32(&buffer_, value.size());
  buffer_.append(key.data() + shared, unshared);
  buffer_.append(value.data(), value.size());
  // update last_key and counter
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, unshared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
