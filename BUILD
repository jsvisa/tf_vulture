# Description:
# Vulture support for TensorFlow.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

load(
    "//tensorflow:tensorflow.bzl",
    "tf_cc_binary",
    "tf_cc_test",
)

tf_cc_binary(
    name = "vulture_file_system.so",
    srcs = [
        "vulture_client.cc",
        "vulture_client.h",
        "vulture_file_system.cc",
        "vulture_file_system.h",
    ],
    copts = ["-Wno-sign-compare"],
    defines = select({
        "//conditions:default": [
            "ENABLE_CURL_CLIENT",
            "ENABLE_NO_ENCRYPTION",
        ],
    }),
    linkshared = 1,
    deps = [
        "//tensorflow/core:framework_headers_lib",
        "@curl",
        "@protobuf_archive//:protobuf_headers",
        "//tensorflow/core/platform/cloud:curl_http_request",
    ],
)

cc_library(
    name = "vulture_client",
    srcs = [
        "vulture_client.cc",
    ],
    hdrs = [
        "vulture_client.h",
    ],
    deps = [
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "@curl",
        "//tensorflow/core/platform/cloud:curl_http_request",
    ],
    alwayslink = 1,
)

cc_library(
    name = "vulture_file_system",
    srcs = [
        "vulture_file_system.cc",
    ],
    hdrs = [
        "vulture_file_system.h",
    ],
    deps = [
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core/platform/cloud:curl_http_request",
    ],
    alwayslink = 1,
)

tf_cc_test(
    name = "vulture_file_system_test",
    size = "small",
    srcs = [
        "vulture_file_system_test.cc",
    ],
    tags = [
        "manual",
    ],
    deps = [
        ":vulture_file_system",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)
