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
#include "include/json/json.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/platform/vulture/vulture_client.h"
#include "tensorflow/core/platform/cloud/curl_http_request.h"

namespace tensorflow {

constexpr char kVultureEOFIter[] = "eyJ0eXBlIjoiZiIsIml0ZXIiOnt9fQ==";
constexpr int kVultureListLimit = 100;

string MaybeAppendSlash(const string& name) {
  if (name.empty()) {
    return "/";
  }
  if (name.back() != '/') {
    return strings::StrCat(name, "/");
  }
  return name;
}

// io::JoinPath() doesn't work in cases when we want an empty subpath
// to result in an appended slash in order for directory markers
// to be processed correctly: "gs://a/b" + "" should give "gs://a/b/".
string JoinPath(const string& path, const string& subpath) {
  return strings::StrCat(MaybeAppendSlash(path), subpath);
}

Status ParseJson(StringPiece json, Json::Value* result) {
  Json::Reader reader;
  if (!reader.parse(json.data(), json.data() + json.size(), *result)) {
    return errors::Internal("Couldn't parse JSON response from Vulture.");
  }
  return Status::OK();
}

Status ParseJson(const std::vector<char>& json, Json::Value* result) {
  return ParseJson(StringPiece{json.data(), json.size()}, result);
}

/// Reads a JSON value with the given name from a parent JSON value.
Status GetValue(const Json::Value& parent, const char* name,
                Json::Value* result) {
  *result = parent.get(name, Json::Value::null);
  if (result->isNull()) {
    return errors::Internal("The field '", name,
                            "' was expected in the JSON response.");
  }
  return Status::OK();
}

/// Reads a string JSON value with the given name from a parent JSON value.
Status GetStringValue(const Json::Value& parent, const char* name,
                      string* result) {
  Json::Value result_value;
  TF_RETURN_IF_ERROR(GetValue(parent, name, &result_value));
  if (!result_value.isString()) {
    return errors::Internal(
        "The field '", name,
        "' in the JSON response was expected to be a string.");
  }
  *result = result_value.asString();
  return Status::OK();
}

/// Reads a long JSON value with the given name from a parent JSON value.
Status GetInt64Value(const Json::Value& parent, const char* name,
                     int64* result) {
  Json::Value result_value;
  TF_RETURN_IF_ERROR(GetValue(parent, name, &result_value));
  if (result_value.isNumeric()) {
    *result = result_value.asInt64();
    return Status::OK();
  }
  if (result_value.isString() &&
      strings::safe_strto64(result_value.asCString(), result)) {
    return Status::OK();
  }
  return errors::Internal(
      "The field '", name,
      "' in the JSON response was expected to be a number.");
}

/// Reads a boolean JSON value with the given name from a parent JSON value.
Status GetBoolValue(const Json::Value& parent, const char* name, bool* result) {
  Json::Value result_value;
  TF_RETURN_IF_ERROR(GetValue(parent, name, &result_value));
  if (!result_value.isBool()) {
    return errors::Internal(
        "The field '", name,
        "' in the JSON response was expected to be a boolean.");
  }
  *result = result_value.asBool();
  return Status::OK();
}


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

// Creates an HttpRequest and sets several parameters that are common to all
// requests.  All code (in GcsFileSystem) that creates an HttpRequest should
// go through this method, rather than directly using http_request_factory_.
Status VultureClient::CreateHttpRequest(std::unique_ptr<HttpRequest>* request) {
  std::unique_ptr<HttpRequest> new_request{http_request_factory_->Create()};

  new_request->AddHeader("User-Agent", "Vulture-TF");

  // TODO: maybe add auth header

  *request = std::move(new_request);
  return Status::OK();
}


Status VultureClient::GetObject(const string &object, int64 offset, int64 n, StringPiece* result, char* scratch) {
  std::unique_ptr<HttpRequest> request;
  TF_RETURN_IF_ERROR(CreateHttpRequest(&request));

  // std::vector<char> response_buffer;
  const string url = JoinPath(this->endpoint_, object);
  request->SetUri(url);
  request->SetRange(offset, offset + n - 1);
  request->SetResultBufferDirect(scratch, n);
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

  *result = StringPiece(scratch, n);

  return Status::OK();
}

Status VultureClient::StatObject(const string &object, FileStatistics *stats) {
  std::unique_ptr<HttpRequest> request;
  TF_RETURN_IF_ERROR(CreateHttpRequest(&request));

  std::vector<char> response_buffer;
  string url = JoinPath(this->endpoint_, object);
  url = strings::StrCat(url, "?stat=true");
  request->SetUri(url);
  request->SetResultBuffer(&response_buffer);
  TF_RETURN_IF_ERROR(request->Send());
  LOG(WARNING) << "Stat Object: " << object;

  uint64 response_code = request->GetResponseCode();

  if (response_code /100 != 2) {
    switch (response_code) {
      case 404:
        return errors::NotFound("Object ", object, " does not exist");
      default:
        return Status(error::INTERNAL, "Stat object is not 2xx");
    }
  }

  StringPiece response = StringPiece(response_buffer.data(), response_buffer.size());

  Json::Value root;
  Json::Reader reader;
  if (!reader.parse(response.begin(), response.end(), root)) {
    return errors::Internal("Couldn't parse JSON response from Vulture server.");
  }

  string type;
  TF_RETURN_WITH_CONTEXT_IF_ERROR(GetStringValue(root, "type", &type),
      " when get type field");
  stats->is_directory = type == "folder";

  TF_RETURN_WITH_CONTEXT_IF_ERROR(GetInt64Value(root, "length", &stats->length),
      " when get length field");

  // TODO: stats->mtime_nsec

  return Status::OK();
}

Status VultureClient::ListObjects(const string &object, std::map<string, FileStatistics>* result) {

  string iter;
  while(true) {
    std::unique_ptr<HttpRequest> request;
    TF_RETURN_IF_ERROR(CreateHttpRequest(&request));

    std::vector<char> response_buffer;
    string url = JoinPath(this->endpoint_, object);
    url = strings::StrCat(url, "?list=true&limit=", kVultureListLimit, "&iter=", iter);
    request->SetUri(url);
    request->SetResultBuffer(&response_buffer);
    TF_RETURN_IF_ERROR(request->Send());
    LOG(WARNING) << "list url is " << url;

    uint64 response_code = request->GetResponseCode();

    if (response_code /100 != 2) {
      return Status(error::INTERNAL, "List objects return not 2xx");
    }

    StringPiece response = StringPiece(response_buffer.data(), response_buffer.size());

    Json::Value root;
    Json::Reader reader;
    if (!reader.parse(response.begin(), response.end(), root)) {
      return errors::Internal("Couldn't parse JSON response from Vulture server.");
    }

    const auto items = root.get("files", Json::Value::null);
    if (!items.isNull()) {
      if (!items.isArray()) {
        return errors::Internal(
            "Expected an array 'files' in the Vulture response.");
      }
      for (size_t i = 0; i < items.size(); i++) {
        const auto item = items.get(i, Json::Value::null);
        if (!item.isObject()) {
          return errors::Internal(
              "Unexpected JSON format: 'files' should be a list of objects.");
        }

        bool deleted = false;
        Status status = GetBoolValue(item, "deleted", &deleted);
        if (status.ok() && deleted) {
          break;
        }

        string name;
        TF_RETURN_IF_ERROR(GetStringValue(item, "name", &name));

        FileStatistics stats;

        string type;
        TF_RETURN_IF_ERROR(GetStringValue(item, "type", &type));
        stats.is_directory = type == "folder";

        TF_RETURN_IF_ERROR(GetInt64Value(item, "length", &stats.length));

        // The name is relative to the 'dirname'. No need to remove the prefix name.
        result->insert(std::make_pair(name, stats));
      }
    }
    TF_RETURN_IF_ERROR(GetStringValue(root, "iter", &iter));

    if (iter == kVultureEOFIter) {
      return Status::OK();
    }
  }
}

}
