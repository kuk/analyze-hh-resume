#!/usr/bin/env python

import sys
import os.path
import json
import cjson
from collections import namedtuple, Counter
from itertools import islice
from random import sample

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib import rc
# For cyrillic labels
rc('font', family='Verdana', weight='normal')


DATA_DIR = 'data'
RESUMES = os.path.join(DATA_DIR, 'resumes.repr')
RESUMES_SAMPLE = os.path.join(DATA_DIR, 'resumes_sample.repr')
TOTAL_RESUMES = 5985469
PARSED_RESUMES = os.path.join(DATA_DIR, 'parsed_resumes.json')


Resume = namedtuple(
    'Resume',
    ['age', 'gender',
     'salary', 'currency',
     'area_id', 'languages',
     'specializations', 'educations']
)


def iterate_chunks(path, chunksize=8192):
    with open(path, 'rb') as file:
        while True:
            chunk = file.read(chunksize)
            if chunk:
                yield chunk
            else:
                break


def iterate_resumes(path=RESUMES):
    OUTSIDE = 0
    INSIDE = 1
    state = OUTSIDE
    buffer = ''
    for chunk in iterate_chunks(path):
        start = 0
        while True:
            if state == OUTSIDE:
                index = chunk.find('{\'desireable_compensation\'', start)
                if index == -1:
                    break
                else:
                    start = index
                    state = INSIDE
            if state == INSIDE:
                index = chunk.find('}, {\'desireable_compensation\'', start)
                if index == -1:
                    buffer += chunk[start:]
                    break
                else:
                    yield buffer + chunk[start:index + 1]
                    buffer = ''
                    start = index
                    state = OUTSIDE
                

def none_or_int(value):
    if value is not None:
        return int(value)


def parse_resume(data):
    age = none_or_int(data['age'])
    gender = data['gender']
    if gender == -1:
        gender = None
    area_id = none_or_int(data['area_id'])
    languages = {}
    for pair in data['language']:
        if pair:
            language, score = pair.split(': ', 1)
            language = int(language)
            score = int(score)
            languages[language] = score
    specializations = [int(_) for _ in data['specialization'] if _]
    educations = [_.decode('utf8') for _ in data['primary_education'] if _]
    return Resume(
        age,
        gender,
        data['desireable_compensation'],
        data['desireable_compensation_currency_code'],
        area_id,
        languages,
        specializations,
        educations
    )


def parse_resumes(data):
    data = eval(data, None, {'nan': None})
    # Since the delimiter between resumes is very long "},
    # {'desireable_compensation" one may need to split data once again
    if isinstance(data, tuple):
        chunks = data
    else:
        chunks = [data]
    for data in chunks:
        yield parse_resume(data)


def read_resumes(path=RESUMES):
    for data in iterate_resumes(path):
        for resume in parse_resumes(data):
            yield resume


def log_progress(stream, every=1000, total=None):
    if total:
        every = total / 200     # every 0.5%
    for index, record in enumerate(stream):
        if index % every == 0:
            if total:
                progress = float(index) / total
                progress = '{0:0.2f}%'.format(progress * 100)
            else:
                progress = index
            print >>sys.stderr, progress,
        yield record


def dump_resume(resume):
    return json.dumps(resume, ensure_ascii=False)


def dump_resumes(resumes):
    with open(PARSED_RESUMES, 'w') as file:
        for resume in resumes:
            dump = dump_resume(resume)
            dump = dump.encode('utf8')
            file.write(dump + '\n')


def load_resume(dump):
    dump = dump.decode('utf8')
    data = cjson.decode(dump)
    (age, gender, salary, currency, area_id,
     languages, specializations, educations) = data
    return Resume(
        age, gender, salary, currency, area_id,
        languages, specializations, educations
    )


def load_resumes():
    with open(PARSED_RESUMES) as file:
        for line in file:
            yield load_resume(line)
