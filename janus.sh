#!/usr/bin/env bash

set -e

http -v POST http://localhost:8749 message="Hello, world!" object:='{"foo": "bar", "names": ["DCjanus", "ZHUANGBISHENG"]}'
