/* Copyright 2018 The Caicloud Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/platform/mutex.h"
#include "tensorflow/core/lib/strings/str_util.h"
#include "tensorflow/core/platform/vulture/vulture_client.h"
#include "tensorflow/core/platform/vulture/vulture_file_system.h"

#include <cstdlib>

namespace tensorflow {

constexpr char kVultureVersion[] = "0.0.1";
constexpr size_t kVultureReadAppendableFileBufferSize = 1024 * 1024;  // In bytes.
// The environment variable that overrides the maximum age of entries in the
// Stat cache. A value of 0 (the default) means nothing is cached.
constexpr char kStatCacheMaxAge[] = "VULTURE_STAT_CACHE_MAX_AGE";
constexpr uint64 kStatCacheDefaultMaxAge = 120;
// The environment variable that overrides
// the maximum number of entries in the Stat cache.
constexpr char kStatCacheMaxEntries[] = "VULTURE_STAT_CACHE_MAX_ENTRIES";
constexpr size_t kStatCacheDefaultMaxEntries = 10240;
// The environment variable that overrides
// the http pool initial size.
constexpr char kPoolSize[] = "VULTURE_POOL_SIZE";
constexpr size_t kPoolDefaultSize = 10;
// The environment variable that overrides
// the http pool overflow size.
constexpr char kPoolOverflow[] = "VULTURE_POOL_OVERFLOW";
constexpr size_t kPoolDefaultOverflow = 2;
// The environment variable that overrides the maximum age of entries in the
// GetMatchingPaths cache. A value of 0 (the default) means nothing is cached.
constexpr char kMatchingPathsCacheMaxAge[] = "VULTURE_MATCHING_PATHS_CACHE_MAX_AGE";
constexpr uint64 kMatchingPathsCacheDefaultMaxAge = 0;
// The environment variable that overrides the maximum number of entries in the
// GetMatchingPaths cache.
constexpr char kMatchingPathsCacheMaxEntries[] =
    "VULTURE_MATCHING_PATHS_CACHE_MAX_ENTRIES";
constexpr size_t kMatchingPathsCacheDefaultMaxEntries = 1024;
// The file statistics returned by Stat() for directories.
const FileStatistics DIRECTORY_STAT(0, 0, true);


namespace {


Status ParseVulturePath(const string& fname, bool empty_object_ok, string* bucket,
                        string* object) {
  if (!bucket || !object) {
    return errors::Internal("bucket and object cannot be null.");
  }
  StringPiece scheme, bucketp, objectp;
  io::ParseURI(fname, &scheme, &bucketp, &objectp);
  if (scheme != "vulture") {
    return errors::InvalidArgument("Vulture path doesn't start with 'vulture://': ",
                                   fname);
  }
  *bucket = bucketp.ToString();
  if (bucket->empty() || *bucket == ".") {
    return errors::InvalidArgument("Vulture path doesn't contain a bucket name: ",
                                   fname);
  }
  objectp.Consume("/");
  *object = objectp.ToString();
  if (!empty_object_ok && object->empty()) {
    return errors::InvalidArgument("Vulture path doesn't contain an object name: ",
                                   fname);
  }
  return Status::OK();
}

class VultureRandomAccessFile : public RandomAccessFile {
 public:
  VultureRandomAccessFile(const string& bucket, const string& object,
                          std::shared_ptr<VultureClient> vulture_client)
      : bucket_(bucket), object_(object), vulture_client_(vulture_client) {}

  Status Read(uint64 offset, size_t n, StringPiece* result,
              char* scratch) const override {
    string object = io::JoinPath(bucket_, object_);
    TF_RETURN_IF_ERROR(this->vulture_client_->GetObject(object, offset, n, result, scratch));
    return Status::OK();
  }

 private:
  string bucket_;
  string object_;
  std::shared_ptr<VultureClient> vulture_client_;
};

class VultureWritableFile : public WritableFile {
 public:
  VultureWritableFile(const string& bucket, const string& object,
                      std::shared_ptr<VultureClient> vulture_client)
      : bucket_(bucket),
        object_(object),
        vulture_client_(vulture_client),
        sync_needed_(true),
        outfile_() {}

  Status Append(const StringPiece& data) override {
    if (!outfile_) {
      return errors::FailedPrecondition(
          "The internal temporary file is not writable.");
    }
    return Status::OK();
  }

  Status Close() override {
    return Status::OK();
  }

  Status Flush() override { return Sync(); }

  Status Sync() override {
    if (!sync_needed_) {
      return Status::OK();
    }
    return Status::OK();
  }

 private:
  string bucket_;
  string object_;
  std::shared_ptr<VultureClient> vulture_client_;
  bool sync_needed_;
  std::shared_ptr<string> outfile_;
};

class VultureReadOnlyMemoryRegion : public ReadOnlyMemoryRegion {
 public:
  VultureReadOnlyMemoryRegion(std::unique_ptr<char[]> data, uint64 length)
      : data_(std::move(data)), length_(length) {}
  const void* data() override { return reinterpret_cast<void*>(data_.get()); }
  uint64 length() override { return length_; }

 private:
  std::unique_ptr<char[]> data_;
  uint64 length_;
};

}  // namespace

// Helper function to extract an environment variable and convert it into a
// value of type T.
template <typename T>
bool GetEnvVar(const char* varname, bool (*convert)(StringPiece, T*), T* value) {
  const char* env_value = std::getenv(varname);
  if (!env_value) {
    return false;
  }
  return convert(env_value, value);
}

// Flushes all caches for filesystem metadata.
// Useful for reclaiming memory once filesystem operations are done (e.g. model is loaded),
// or for resetting the filesystem to a consistent state.
void VultureFileSystem::FlushCaches() {
  stat_cache_->Clear();
  matching_paths_cache_->Clear();
}

VultureFileSystem::VultureFileSystem()
  : client_lock_() {
  uint64 value;

  LOG(WARNING) << "Init the vulutre file system version: " << kVultureVersion << "..............";

  // Apply overrides for the stat cache max age and max entries, if provided.
  uint64 stat_cache_max_age = kStatCacheDefaultMaxAge;
  size_t stat_cache_max_entries = kStatCacheDefaultMaxEntries;
  if (GetEnvVar(kStatCacheMaxAge, strings::safe_strtou64, &value)) {
    stat_cache_max_age = value;
  }
  if (GetEnvVar(kStatCacheMaxEntries, strings::safe_strtou64, &value)) {
    stat_cache_max_entries = value;
  }
  stat_cache_.reset(new ExpiringLRUCache<FileStatistics>(
      stat_cache_max_age, stat_cache_max_entries));

  // Apply overrides for the matching paths cache max age and max entries, if
  // provided.
  uint64 matching_paths_cache_max_age = kMatchingPathsCacheDefaultMaxAge;
  size_t matching_paths_cache_max_entries =
      kMatchingPathsCacheDefaultMaxEntries;
  if (GetEnvVar(kMatchingPathsCacheMaxAge, strings::safe_strtou64, &value)) {
    matching_paths_cache_max_age = value;
  }
  if (GetEnvVar(kMatchingPathsCacheMaxEntries, strings::safe_strtou64,
                &value)) {
    matching_paths_cache_max_entries = value;
  }
  matching_paths_cache_.reset(new ExpiringLRUCache<std::vector<string>>(
      matching_paths_cache_max_age, matching_paths_cache_max_entries));

  size_t pool_size = kPoolDefaultSize;
  if (GetEnvVar(kPoolSize, strings::safe_strtou64, &value)) {
    pool_size = value;
  }
  pool_size_ = pool_size;

  size_t pool_overflow = kPoolDefaultOverflow;
  if (GetEnvVar(kPoolOverflow, strings::safe_strtou64, &value)) {
    pool_overflow = value;
  }
  pool_overflow_ = pool_overflow;

  while (pool_.size() < pool_size_) {
    auto client = std::shared_ptr<VultureClient>(new VultureClient());
    pool_.push_back(client);
  }
}

VultureFileSystem::~VultureFileSystem() {
  while (pool_.size()) {
    pool_.pop_front();
  }
}

std::shared_ptr<VultureClient> VultureFileSystem::GetVultureClient() {
  std::lock_guard<mutex> lock(this->client_lock_);

  // LOG(WARNING) << "Before GET, current pool size: " << pool_.size();
  if (this->pool_.size()) {
    std::shared_ptr<VultureClient> client = this->pool_.front();
    this->pool_.pop_front();
    return client;
  }

  // FIXME: limit the number of temp client
  return std::shared_ptr<VultureClient>(new VultureClient());
}

void VultureFileSystem::PutVultureClient(std::shared_ptr<VultureClient> client) {
  // LOG(WARNING) << "Before PUT, Current pool size: " << pool_.size();
  std::lock_guard<mutex> lock(this->client_lock_);
  this->pool_.push_back(client);
}

Status VultureFileSystem::NewRandomAccessFile(
    const string& fname, std::unique_ptr<RandomAccessFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, false, &bucket, &object));

  auto client = GetVultureClient();
  result->reset(new VultureRandomAccessFile(bucket, object, client));
  PutVultureClient(client);

  return Status::OK();
}

Status VultureFileSystem::NewWritableFile(
    const string& fname, std::unique_ptr<WritableFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, false, &bucket, &object));

  auto client = GetVultureClient();
  result->reset(new VultureWritableFile(bucket, object, client));
  PutVultureClient(client);

  return Status::OK();
}

Status VultureFileSystem::NewAppendableFile(
    const string& fname, std::unique_ptr<WritableFile>* result) {
  std::unique_ptr<RandomAccessFile> reader;
  TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &reader));
  std::unique_ptr<char[]> buffer(new char[kVultureReadAppendableFileBufferSize]);
  Status status;
  uint64 offset = 0;
  StringPiece read_chunk;

  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, false, &bucket, &object));

  auto client = GetVultureClient();
  result->reset(new VultureWritableFile(bucket, object, client));
  PutVultureClient(client);

  while (true) {
    status = reader->Read(offset, kVultureReadAppendableFileBufferSize, &read_chunk, buffer.get());
    if (status.ok()) {
      (*result)->Append(read_chunk);
      offset += kVultureReadAppendableFileBufferSize;
    } else if (status.code() == error::OUT_OF_RANGE) {
      (*result)->Append(read_chunk);
      break;
    } else {
      (*result).reset();
      return status;
    }
  }

  return Status::OK();
}

Status VultureFileSystem::NewReadOnlyMemoryRegionFromFile(
    const string& fname, std::unique_ptr<ReadOnlyMemoryRegion>* result) {
  uint64 size;
  TF_RETURN_IF_ERROR(GetFileSize(fname, &size));
  std::unique_ptr<char[]> data(new char[size]);

  std::unique_ptr<RandomAccessFile> file;
  TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &file));

  StringPiece piece;
  TF_RETURN_IF_ERROR(file->Read(0, size, &piece, data.get()));

  result->reset(new VultureReadOnlyMemoryRegion(std::move(data), size));
  return Status::OK();
}

Status VultureFileSystem::FileExists(const string& fname) {
  FileStatistics stats;
  TF_RETURN_IF_ERROR(this->Stat(fname, &stats));
  return Status::OK();
}

Status VultureFileSystem::GetChildren(const string& dir, std::vector<string>* result) {
  string bucket, prefix;
  TF_RETURN_IF_ERROR(ParseVulturePath(dir, false, &bucket, &prefix));

  if (prefix.back() != '/') {
    prefix.push_back('/');
  }

  string object = io::JoinPath(bucket, prefix);
  LOG(WARNING) << "List children: " << dir;

  std::map<string, FileStatistics> objects;

  auto client = GetVultureClient();
  VULTURE_RETURN_IF_ERROR(client->ListObjects(object, &objects), PutVultureClient(client));

  // Insert every object into `stat_cache_'
  std::map<std::string, FileStatistics>::iterator it = objects.begin();
  while(it != objects.end()) {
    result->push_back(it->first);
    string key = io::JoinPath(object, it->first);
    LOG(WARNING) << "Insert stat_cache : " << key;
    this->stat_cache_->Insert(key, it->second);
    it++;
  }

  return Status::OK();
}

Status VultureFileSystem::Stat(const string& fname, FileStatistics* stats) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, true, &bucket, &object));

  object = io::JoinPath(bucket, object);

  auto client = GetVultureClient();
  StatCache::ComputeFunc compute_func = [this, &object, &client](
                                            const string& fname,
                                            FileStatistics* stats) {
    VULTURE_RETURN_IF_ERROR(client->StatObject(object, stats), PutVultureClient(client));

    return Status::OK();
  };

  TF_RETURN_IF_ERROR(this->stat_cache_->LookupOrCompute(object, stats, compute_func));
  return Status::OK();
}

Status VultureFileSystem::GetMatchingPaths(const string& pattern,
                                           std::vector<string>* results) {
  return Status::OK();
}

Status VultureFileSystem::DeleteFile(const string& fname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, false, &bucket, &object));

  // TODO: deleteFile
  return Status::OK();
}

Status VultureFileSystem::CreateDir(const string& dirname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(dirname, true, &bucket, &object));

  // TODO: CreateDir
  return Status::OK();
}

Status VultureFileSystem::DeleteDir(const string& dirname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(dirname, false, &bucket, &object));

  // TODO: listObjects than delete one by one
  return Status::OK();
}

Status VultureFileSystem::GetFileSize(const string& fname, uint64* file_size) {
  FileStatistics stats;
  TF_RETURN_IF_ERROR(this->Stat(fname, &stats));
  *file_size = stats.length;
  return Status::OK();
}

Status VultureFileSystem::RenameFile(const string& src, const string& dst) {
  string src_bucket, src_object, dst_bucket, dst_object;
  TF_RETURN_IF_ERROR(ParseVulturePath(src, false, &src_bucket, &src_object));
  TF_RETURN_IF_ERROR(ParseVulturePath(dst, false, &dst_bucket, &dst_object));
  if (src_object.back() == '/') {
    if (dst_object.back() != '/') {
      dst_object.push_back('/');
    }
  } else {
    if (dst_object.back() == '/') {
      dst_object.pop_back();
    }
  }

  // TODO: copy to dest, then delete src
  return Status::OK();
}

REGISTER_FILE_SYSTEM("vulture", VultureFileSystem);

}  // namespace tensorflow
