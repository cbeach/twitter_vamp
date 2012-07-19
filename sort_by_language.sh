#!/bin/bash

rm data/sorted_archive
rm data/sorted_archives

python dump_archive.py &
python nlp/sort_by_lang.py * &


java -jar nlp/DetectLang/dist/DetectLang.jar &
java -jar nlp/DetectLang/dist/DetectLang.jar &
java -jar nlp/DetectLang/dist/DetectLang.jar &
java -jar nlp/DetectLang/dist/DetectLang.jar &

