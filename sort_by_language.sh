#!/bin/bash

rm data/sorted_archive
rm data/sorted_archives

python nlp/sort_by_lang.py * &
java -jar nlp/DetectLang/dist/DetectLang.jar &
python dump_archive.py &
