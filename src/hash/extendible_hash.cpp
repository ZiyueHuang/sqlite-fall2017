#include <list>
#include <cassert>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size): global_depth_(0), size_limit_(size) {
    bucket_directory_.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template<typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    return std::hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template<typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template<typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_index) const {
    return bucket_directory_[bucket_index]->local_depth_;
}

/*
 * helper function to return current number of bucket in hash table
 */
template<typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    return static_cast<int>(bucket_directory_.size());
}

/*
 * lookup function to find value associate with input key
 */
template<typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::lock_guard<std::mutex> guard(latch_);
    std::shared_ptr<Bucket> bucket = GetBucket(key);
    if (bucket->map_.find(key) == bucket->map_.end()) {
        return false;
    }
    value = bucket->map_[key];
    return true;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template<typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    std::lock_guard<std::mutex> guard(latch_);
    std::shared_ptr<Bucket> bucket = GetBucket(key);
    if (bucket->map_.find(key) == bucket->map_.end()) {
        return false;
    }
    bucket->map_.erase(key);
    return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template<typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    std::lock_guard<std::mutex> guard(latch_);
    std::shared_ptr<Bucket> bucket = GetBucket(key);
    while (bucket->map_.size() == size_limit_) {
        if (bucket->local_depth_ == global_depth_) {
            size_t length = bucket_directory_.size();
            for (size_t i = 0; i < length; i++){
                bucket_directory_.push_back(bucket_directory_[i]);
            }
            global_depth_++;
        }
        int mask = 1 << bucket->local_depth_;
        auto left_bucket = std::make_shared<Bucket>(bucket->local_depth_ + 1);
        auto right_bucket = std::make_shared<Bucket>(bucket->local_depth_ + 1);
        for (auto item : bucket->map_){
            if (mask & HashKey(item.first)){
                right_bucket->map_.insert(item);
            } else {
                left_bucket->map_.insert(item);
            }
        }
        for (size_t i = 0; i < bucket_directory_.size(); i++){
            if (bucket_directory_[i] == bucket){
                if (mask & i){
                    bucket_directory_[i] = right_bucket;
                } else {
                    bucket_directory_[i] = left_bucket;
                }
            }
        }
        bucket = GetBucket(key);
    }
    bucket->map_[key] = value;
}

template<typename K, typename V>
int ExtendibleHash<K, V>::GetBucketIndex(size_t hash_value) const {
    return static_cast<int>(hash_value & ((1 << global_depth_) - 1));
}

template<typename K, typename V>
std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> ExtendibleHash<K, V>::GetBucket(const K &key) {
    return bucket_directory_.at(GetBucketIndex(HashKey(key)));
};

template class ExtendibleHash<page_id_t, Page *>;

template class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
template class ExtendibleHash<int, std::string>;

template class ExtendibleHash<int, std::list<int>::iterator>;

template class ExtendibleHash<int, int>;
} // namespace cmudb
