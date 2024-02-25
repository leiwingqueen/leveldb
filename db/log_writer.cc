// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

// hint:
// - block_offset_ is the offset of the current block
// - if left size of block is smaller than the kHeaderSize,we need to append 0x00 to the end of the block
// - use EmitPhysicalRecord to write record to the file
// - consider: why we need to split record as 32kB when we append record to the file
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record

  // TODO: implement add record
  return Status::OK();
}

// hint:
// - use crc32c::Extend(type_crc_[t], ptr, length) to generate the crc code.
// - use crc32c::Mask(crc) to generate 4 byte crc code
// - header format: crc(4 byte)|length(2 byte)|type
// - use dest_->append to append data to file, call the flush function after that append data to file
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
        assert(length <= 0xffff);  // Must fit in two bytes
        assert(block_offset_ + kHeaderSize + length <= kBlockSize);

        // Format the header
        // calculate the crc32
        uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
        crc = crc32c::Mask(crc);
        char header[kHeaderSize];
        for (int i = 0; i < 4; ++i) {
            header[i] = static_cast<char>(crc & 0xFF);
            crc >>= 8;
        }
        size_t size = length;
        for (int i = 4; i < 6; ++i) {
            header[i] = static_cast<char>(size & 0xFF);
            size >>= 8;
        }
        header[6] = static_cast<char>(t);
        // write header and content
        Status status = dest_->Append(Slice(header, kHeaderSize));
        if (!status.ok()){
            return status;
        }
        status=dest_->Append(Slice(ptr, length));
        return status;
}

}  // namespace log
}  // namespace leveldb
