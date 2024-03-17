// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {
// implement build table
// hint:
// - meta->number: get the current filename of database
// - TableBuilder* builder = new TableBuilder(options, file); use this statement
// to build a new builder
// - use iter to scan all the data in memtable, and add key value data into
// builder
// - you need to set the smallest and biggest key in metaData. use decodeFrom
// function achieve that
// - after add all the key value data into database. you need to flush the file
// and sync.
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string fileName = TableFileName(dbname, meta->number);
  Status s;
  if (iter->Valid()) {
    // open a file
    WritableFile* file;
    s = env->NewWritableFile(fileName, &file);
    if (!s.ok()) {
      return s;
    }
    // build a table build for write key value into file
    TableBuilder* builder = new TableBuilder(options, file);
    // we need to set the meta data
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      Slice value = iter->value();
      builder->Add(key, value);
      meta->largest.DecodeFrom(key);
    }
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fileName);
  }
  return s;
}

}  // namespace leveldb
