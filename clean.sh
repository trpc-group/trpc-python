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
rm -rf trpc.egg-info
rm -rf trpc/codec/*.so
rm -rf trpc/codec/*.h
rm -rf trpc/codec/*.c
rm -rf trpc/codec/*.cpp

rm -rf trpc/driver_trpcio_ext/*.cpp
rm -rf trpc/driver_trpcio_ext/*.so

rm -rf trpc/lib/ 
rm -rf trpc/ml/ext/*.cpp
rm -rf trpc/ml/ext/*.so


rm -rf trpc/protocol/*.c
rm -rf trpc/protocol/*.so

rm -rf trpc/server/*.cpp
rm -rf trpc/server/*.c
rm -rf trpc/server/*.so

rm -rf trpc/task/python/*.so
rm -rf trpc/task/*.so
rm -rf trpc/task/*.cpp
rm -rf trpc/task/*.h

rm -rf trpc/transport/*.so
rm -rf trpc/transport/*.cpp
rm -rf trpc/transport_trpcio/*.so
rm -rf trpc/transport_trpcio/*.cpp

rm -rf trpc/trpc_abc/*.c
rm -rf trpc/trpc_abc/*.so

rm -rf trpc/*.c
rm -rf trpc/*.so

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

pip3 uninstall -y trpc

