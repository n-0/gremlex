#!/bin/bash

export MIX_ENV=test

mix deps.get && \
    mix compile --warnings-as-errors && \
    mix coveralls --trace
