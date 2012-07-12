#!/bin/bash

python fetcher.py file &
python parse.py 3 &

java -jar nlp/TwitterFeed.jar
java -jar nlp/LangDetect.jar


