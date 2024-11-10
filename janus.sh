#!/usr/bin/env bash

set -e

http POST http://localhost:8749 message="Hello, world!"
