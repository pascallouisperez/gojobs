## Gojobs — Guaranteed Jobs on top of MySQL

[![Build Status](https://travis-ci.org/pascallouisperez/gojobs.svg?branch=master)](https://travis-ci.org/pascallouisperez/gojobs)
[![GoDoc](https://godoc.org/github.com/pascallouisperez/gojobs?status.svg)](https://godoc.org/github.com/pascallouisperez/gojobs)

_In active development._

## TODO

* Metrics
* Swap marshaller
* Fetcher-Processor architecture

# Implementation Details

## Job State Machine

remaining updated when locking

# Developing

Example of running a single test

    go test -timeout 10s -v github.com/pascallouisperez/gojobs -logtostderr=true -check.f 'TestReEnqueue'
