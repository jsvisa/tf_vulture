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
#ifndef TENSORFLOW_CORE_PLATFORM_VULTURE_VULTURE_CLIENT_H_
#define TENSORFLOW_CORE_PLATFORM_VULTURE_VULTURE_CLIENT_H_
#include "tensorflow/core/platform/vulture/vulture_http_request.h"

namespace tensorflow {
class VultureClient {
  public:
    VultureClient();
    explicit VultureClient(
        std::shared_ptr<HttpRequest::Factory> http_request_factory,
        Env *env);
    ~VultureClient();

    Status GetObject(const string &object, int64 offset, int64 n, StringPiece* result, char* scratch);
    Status StatObject(const string &object, FileStatistics *stats);
    Status ListObjects(const string &object, std::map<string, FileStatistics>* result);

  private:
    std::shared_ptr<HttpRequest> CreateHttpRequest();
    std::shared_ptr<HttpRequest::Factory> http_request_factory_;
    std::shared_ptr<HttpRequest> request_;
    Env* env_;
    string endpoint_;
    long connectTimeoutMs_;
    long requestTimeoutMs_;
};
} // namespace tensorflow

#endif
