# TensorFlow Vulture Plugin

> Current ONLY support TensorFlow 1.7

**What is Vulture?**
> Vulture is a S3 like K-V Storage, see more from https://github.com/caicloud/vulture

## How to build along with TensorFlow

### Prepare the environment

```
$ git clone https://github.com/tensorflow/tensorflow
$ git clone https://github.com/jsvisa/tf_vulture
$ cd tensorflow && git checkout v1.7.0
$ ln -snf `pwd`/../tf_vulture `pwd`/tensorflow/core/platform/vulture
```

### Build TensorFlow first

```
$ cd /<path/to/tensorflow>
$ ./configure && bazel build --config=opt //tensorflow/tools/pip_package:build_pip_package
$ (install tensorflow python package)
```

### Build the tf_vulture plugin

```
$ cd /<path/to/tensorflow>/tensorflow/core/platform/vulture
$ bazel build --config opt :vulture_file_system.so
```

## TODO

- [ ] how to use it?
- [ ] test case
- [ ] performance benchmark
- [ ] provide vulture_file_system.so, based on Ubuntu, CentOS
