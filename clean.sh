#!/bin/sh

rm -rf coverage.xml
rm -rf trpc.log.gz
rm -rf *.log
rm -rf htmlcov
rm -rf build
rm -rf dist
rm -rf cmake_third_party/
rm -rf .coverage
rm -rf .__*


rm -rf trpc_sdk.egg-info
rm -rf trpc_python.egg-info
find trpc_sdk -type f \( -name '*.so' -o -name '*.c' -o -name '*.cpp' \) -delete

rm -rf examples/*.log
rm -rf examples/.__trpc*
rm -rf examples/helloworld/*.log
rm -rf examples/helloworld/.__trpc*
rm -rf examples/helloworld/.__hello*

rm -rf test_tracemalloc*
rm -rf test-ngtest-ut-trpc-python*
rm -rf cov.tmp
rm -rf trpc_local



find -type d | grep __pycache__ | xargs rm -r

pip3 uninstall -y trpc_python

