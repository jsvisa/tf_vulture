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
#include "tensorflow/core/platform/vulture/vulture_client.h"
#include "tensorflow/core/platform/vulture/vulture_file_system.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/str_util.h"
// #include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/mutex.h"

#include <cstdlib>

namespace tensorflow {

static const size_t kVultureReadAppendableFileBufferSize = 1024 * 1024;
static const int kVultureGetChildrenMaxKeys = 100;

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
    // LOG(INFO) << "Read " << object << " with offset: " << offset << " and size: " << n;
    TF_RETURN_IF_ERROR(this->vulture_client_->GetObject(
          object, offset, offset + n - 1, result, scratch));
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

VultureFileSystem::VultureFileSystem()
  : vulture_client_(nullptr), client_lock_() {}

VultureFileSystem::~VultureFileSystem() {}

// Initializes vulture_client_, if needed, and returns it.
std::shared_ptr<VultureClient> VultureFileSystem::GetVultureClient() {
  std::lock_guard<mutex> lock(this->client_lock_);

  // TODO: lock?
  if (this->vulture_client_ == nullptr) {
    this->vulture_client_ = std::shared_ptr<VultureClient>(new VultureClient());
  }

  return this->vulture_client_;
}

Status VultureFileSystem::NewRandomAccessFile(
    const string& fname, std::unique_ptr<RandomAccessFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, false, &bucket, &object));
  result->reset(new VultureRandomAccessFile(bucket, object, this->GetVultureClient()));
  return Status::OK();
}

Status VultureFileSystem::NewWritableFile(
    const string& fname, std::unique_ptr<WritableFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, false, &bucket, &object));
  result->reset(new VultureWritableFile(bucket, object, this->GetVultureClient()));
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
  result->reset(new VultureWritableFile(bucket, object, this->GetVultureClient()));

  while (true) {
    status = reader->Read(offset, kVultureReadAppendableFileBufferSize, &read_chunk,
                          buffer.get());
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

  TF_RETURN_IF_ERROR(this->GetVultureClient()->ListObjects(
        object, result));
  return Status::OK();
}

Status VultureFileSystem::Stat(const string& fname, FileStatistics* stats) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseVulturePath(fname, true, &bucket, &object));

  object = io::JoinPath(bucket, object);

  TF_RETURN_IF_ERROR(this->GetVultureClient()->StatObject(
        object, stats));

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

Status VultureFileSystem::RenameFile(const string& src, const string& target) {
  string src_bucket, src_object, target_bucket, target_object;
  TF_RETURN_IF_ERROR(ParseVulturePath(src, false, &src_bucket, &src_object));
  TF_RETURN_IF_ERROR(
      ParseVulturePath(target, false, &target_bucket, &target_object));
  if (src_object.back() == '/') {
    if (target_object.back() != '/') {
      target_object.push_back('/');
    }
  } else {
    if (target_object.back() == '/') {
      target_object.pop_back();
    }
  }

  // TODO: copy to dest, then delete src
  return Status::OK();
}

REGISTER_FILE_SYSTEM("vulture", VultureFileSystem);

}  // namespace tensorflow
