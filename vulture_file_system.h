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
#ifndef TENSORFLOW_CORE_PLATFORM_VULTURE_VULTURE_FILE_SYSTEM_H_
#define TENSORFLOW_CORE_PLATFORM_VULTURE_VULTURE_FILE_SYSTEM_H_

#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/mutex.h"
#include "tensorflow/core/platform/vulture/vulture_client.h"

namespace tensorflow {

class VultureFileSystem : public FileSystem {
 public:
  VultureFileSystem();
  ~VultureFileSystem();

  Status NewRandomAccessFile(
      const string& fname, std::unique_ptr<RandomAccessFile>* result) override;

  Status NewWritableFile(const string& fname,
                         std::unique_ptr<WritableFile>* result) override;

  Status NewAppendableFile(const string& fname,
                           std::unique_ptr<WritableFile>* result) override;

  Status NewReadOnlyMemoryRegionFromFile(
      const string& fname,
      std::unique_ptr<ReadOnlyMemoryRegion>* result) override;

  Status FileExists(const string& fname) override;

  Status GetChildren(const string& dir, std::vector<string>* result) override;

  Status Stat(const string& fname, FileStatistics* stat) override;

  Status GetMatchingPaths(const string& pattern,
                          std::vector<string>* results) override;

  Status DeleteFile(const string& fname) override;

  Status CreateDir(const string& name) override;

  Status DeleteDir(const string& name) override;

  Status GetFileSize(const string& fname, uint64* size) override;

  Status RenameFile(const string& src, const string& target) override;

 private:
  // Returns the member Vulture client, initializing as-needed.
  // When the client tries to access the object in Vulture, e.g.,
  //   vulture://bucket-name/path/to/object
  // the behavior could be controlled by various environmental
  // variables.
  // The endpoint could be overridden explicitly with `VULTURE_ENDPOINT`.
  // This Vulture Client does not support Virtual Hosted–Style Method
  // for a bucket.
  std::shared_ptr<VultureClient> GetVultureClient();

  std::shared_ptr<VultureClient> vulture_client_;
  // Lock held when checking for vulture_client_ initialization.
  mutex client_lock_;
};

}  // namespace tensorflow

#endif  // TENSORFLOW_CONTRIB_VULTURE_VULTURE_FILE_SYSTEM_H_
