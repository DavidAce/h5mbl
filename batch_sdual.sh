#!/bin/bash

prefix='data146'

#./cmake-build-release-clang-10/h5mbl -n merged_multi1.h5 -o output_multi1 -t output -s $prefix -V2 -v2 -f
#./cmake-build-release-clang-10/h5mbl -n merged_multi2.h5 -o output_grow_multi2 -t output -s $prefix -V2 -v2 -f
./cmake-build-release-clang-10/h5mbl -M sdual -n merged_multi2.h5 -o output -t output -s $prefix -V2 -v2 -f
#./cmake-build-release-clang-10/h5mbl -n merged_multi6.h5 -o output_multi6 -t output -s $prefix -V2 -v2 -f
