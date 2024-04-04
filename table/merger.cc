// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    // Implement this method.
    // Hint: You can use the FindSmallest() method.
    // Hint: You can use the SeekToFirst() method of the children.
    // Hint: remember to set the direction to kForward.
    for (int i = 0; i < n_; ++i) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    // Implement this method.
    // Hint: You can use the FindLargest() method.
    // Hint: You can use the SeekToLast() method of the children.
    // Hint: remember to set the direction to kReverse.
    for (int i = 0; i < n_; ++i) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    // Implement this method
    // Hint: You can use the Seek() method of the children.
    // Hint: remember to set the direction to kForward.
    // Hint: remember to call FindSmallest() after seeking.
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.

    // Implement this method.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; ++i) {
        IteratorWrapper* iter = &children_[i];
        iter->Seek(key());
        if (current_ != iter && iter->Valid() &&
            comparator_->Compare(iter->key(), key()) == 0) {
          iter->Next();
        }
      }
    }
    current_->Next();
    FindSmallest();
    direction_ = kForward;
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.

    // Implement this method.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; ++i) {
        IteratorWrapper* iter = &children_[i];
        // why we need to seek first?
        iter->Seek(key());
        if (current_ != iter) {
          if (!iter->Valid()) {
            iter->SeekToLast();
          } else {
            iter->Prev();
          }
        }
      }
    }
    current_->Prev();
    FindLargest();
    direction_ = kReverse;
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  // Implement this method.
  // Hint: You can use the key() method of the children.
  // Hint: You can use the comparator_ to compare the keys.
  // Hint: You can use the Valid() method of the children.
  // Hint: You need to set the current_ to the smallest child.
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; ++i) {
    IteratorWrapper* iter = &children_[i];
    if (iter->Valid()) {
      if (smallest == nullptr ||
          comparator_->Compare(iter->key(), smallest->key()) < 0) {
        smallest = iter;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  // Implement this method.
  // Hint: You can use the key() method of the children.
  // Hint: You can use the comparator_ to compare the keys.
  // Hint: You can use the Valid() method of the children.
  // Hint: You need to set the current_ to the largest child.
  IteratorWrapper* largest = nullptr;
  for (int i = 0; i < n_; ++i) {
    IteratorWrapper* iter = &children_[i];
    if (iter->Valid()) {
      if (largest == nullptr ||
          comparator_->Compare(iter->key(), largest->key()) > 0) {
        largest = iter;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
