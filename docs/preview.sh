#!/bin/bash

echo "Visit your site at http://localhost:4000"

bundler exec jekyll serve --watch --incremental --baseurl '' $@

