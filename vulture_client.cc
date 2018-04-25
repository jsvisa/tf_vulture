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
#include "tensorflow/core/platform/vulture/vulture_client.h"
#include "tensorflow/core/platform/cloud/curl_http_request.h"

namespace tensorflow {
VultureClient::VultureClient()
    : VultureClient(
        std::unique_ptr<HttpRequest::Factory>(new CurlHttpRequest::Factory()),
        Env::Default()
        ) {}

VultureClient::VultureClient(
    std::unique_ptr<HttpRequest::Factory> http_request_factory,
    Env* env)
    : http_request_factory_(std::move(http_request_factory)), env_(env) {

  this->endpoint_ = "http://127.0.0.1:3120";
  const char* endpoint = getenv("VULTURE_ENDPOINT");
  if (endpoint) {
    this->endpoint_ = string(endpoint);
  }

  int64 timeout;

  this->connectTimeoutMs_ = 500;
  const char* connect_timeout = getenv("VULTURE_CONNECT_TIMEOUT_MSEC");
  if (connect_timeout) {
    if (strings::safe_strto64(connect_timeout, &timeout)) {
      this->connectTimeoutMs_ = timeout;
    }
  }

  this->requestTimeoutMs_ = 5000;
  const char* request_timeout = getenv("VULTURE_REQUEST_TIMEOUT_MSEC");
  if (request_timeout) {
    if (strings::safe_strto64(request_timeout, &timeout)) {
      this->requestTimeoutMs_ = timeout;
    }
  }

}


VultureClient::~VultureClient() {}


Status VultureClient::GetObject(const string &object, int64 start, int64 end, StringPiece* result, char* scratch) {
  std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
  std::vector<char> response_buffer;
  const string url = io::JoinPath(this->endpoint_, object);
  const string bytes = strings::StrCat("bytes=", start, "-", end);
  request->SetUri(url);
  request->AddHeader("User-Agent", "Vulture-TF");
  request->AddHeader("Range", bytes);
  request->SetResultBuffer(&response_buffer);
  TF_RETURN_IF_ERROR(request->Send());

  uint64 response_code = request->GetResponseCode();

  if (response_code /100 != 2) {
    switch (response_code) {
      case 404:
        return errors::NotFound("Object ", object, " does not exist");
      default:
        string msg = strings::StrCat("Get Object ", url, " status is not 2xx(", response_code, ")");
        return Status(error::INTERNAL, msg);
    }
  }

  int n = response_buffer.size();
  std::stringstream ss;
  ss << response_buffer[0];
  ss.read(scratch, n);

  *result = StringPiece(scratch, n);

  return Status::OK();
}

Status VultureClient::StatObject(const string &object, int64 *result) {
  std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
  std::vector<char> response_buffer;
  const string url = io::JoinPath(this->endpoint_, object);
  request->SetUri(url);
  request->AddHeader("User-Agent", "Vulture-TF");
  request->SetResultBuffer(&response_buffer);
  TF_RETURN_IF_ERROR(request->Send());

  uint64 response_code = request->GetResponseCode();

  if (response_code /100 != 2) {
    switch (response_code) {
      case 404:
        return errors::NotFound("Object ", object, " does not exist");
      default:
        return Status(error::INTERNAL, "GET object is not 2xx");
    }
  }

  int64 n;

  string length = request->GetResponseHeader("Content-Length");
  if (strings::safe_strto64(length, &n)) {
    *result = n;
  }

  return Status::OK();
}

}
