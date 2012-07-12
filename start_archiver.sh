#!/bin/bash

python core/fetcher.py &
python core/archiver.py raw &
