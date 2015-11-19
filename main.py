#!/usr/bin/env python
# encoding: utf8

import sys
import os.path
import json
import cjson
import base64
from collections import defaultdict, namedtuple, Counter
from itertools import islice
from random import sample, random

import requests
requests.packages.urllib3.disable_warnings()

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib import rc
# For cyrillic labels
rc('font', family='Verdana', weight='normal')


DATA_DIR = 'data'
RAW_RESUMES = os.path.join(DATA_DIR, 'resumes.repr')
TOTAL_RESUMES = 5985469
RESUMES = os.path.join(DATA_DIR, 'resumes.json')
AREAS = os.path.join(DATA_DIR, 'areas.json')
SCHOOLS = os.path.join(DATA_DIR, 'schools.json')
UNIVERSITIES_DIR = os.path.join(DATA_DIR, 'universities')
UNIVERSITIES = os.path.join(DATA_DIR, 'universities.xlsx')
VACANCIES = os.path.join(DATA_DIR, 'vacancies.json')
TOTAL_VACANCIES = 302374


Resume = namedtuple(
    'Resume',
    ['age', 'gender',
     'salary', 'currency',
     'area_id', 'languages',
     'specializations', 'educations']
)
Area = namedtuple('Area', ['id', 'parent_id', 'level', 'name'])
Salary = namedtuple('Salary', ['min', 'max', 'currency'])
Profarea = namedtuple('Profarea', ['id', 'name'])
Specialization = namedtuple('Specialization', ['group', 'id', 'name'])
Vacancy= namedtuple('Vacancy', ['area_id', 'salary', 'specializations'])


def iterate_chunks(path, chunksize=8192):
    with open(path, 'rb') as file:
        while True:
            chunk = file.read(chunksize)
            if chunk:
                yield chunk
            else:
                break


def iterate_resumes(path=RAW_RESUMES):
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


def parse_salary(data):
    salary = data['salary']
    if salary is not None:
        min = salary['from']
        max = salary['to']
        currency = salary['currency']
        return Salary(min, max, currency)


def parse_specializations(data):
    for specialization in data['specializations']:
        id = int(specialization['profarea_id'])
        name = specialization['profarea_name']
        group = Profarea(id, name)
        group_id, id = specialization['id'].split('.', 1)
        assert int(group_id) == group.id, data['alternate_url']
        id = int(id)
        name = specialization['name']
        yield Specialization(group, id, name)


def iterate_vacancies():
    with open(VACANCIES) as file:
        for line in file:
            yield json.loads(line)


def read_vacancies():
    for data in iterate_vacancies():
        area_id = int(data['area']['id'])
        salary = parse_salary(data)
        specializations = list(parse_specializations(data))
        yield Vacancy(area_id, salary, specializations)


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
    with open(RESUMES, 'w') as file:
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
    with open(RESUMES) as file:
        for line in file:
            yield load_resume(line)


def show_age_distribution(resumes):
    data = Counter()
    total = 0
    undefined = 0
    unbound = 0
    for resume in resumes:
        total += 1
        age = resume.age
        if age is None:
            undefined += 1
        else:
            if 15 < age < 80:
                data[age] += 1
            else:
                unbound += 1
    fig, ax = plt.subplots()
    table = pd.Series(data)
    table.plot(ax=ax)
    ax.set_xlabel(u'Возраст')
    print 'Undefined: {0:0.2f}%'.format(float(undefined) / total * 100)
    print 'Unbound: {0:0.2f}%'.format(float(unbound) / total * 100)


def show_gender_distribution(resumes):
    data = Counter()
    total = 0
    undefined = 0
    for resume in resumes:
        total += 1
        gender = resume.gender
        if gender is None:
            undefined += 1
        else:
            data[gender] += 1
    fig, ax = plt.subplots()
    table = pd.Series(data)
    table.plot(ax=ax, kind='bar')
    ax.set_xlabel(u'Пол')
    print 'Undefined: {0:0.2f}%'.format(float(undefined) / total * 100)


def show_currency_distribution(resumes):
    data = Counter()
    total = 0
    rur = 0
    undefined = 0
    for resume in resumes:
        total += 1
        currency = resume.currency
        if currency is None:
            undefined += 1
        else:
            if currency == 'RUR':
                rur += 1
            data[currency] += 1
    fig, ax = plt.subplots()
    table = pd.Series(data)
    table = table.sort_values(ascending=False)
    table.plot(ax=ax, kind='bar')
    print 'Undefined: {0:0.2f}%'.format(float(undefined) / total * 100)
    print 'RUR from defined: {0:0.2f}%'.format(float(rur) / (total - undefined) * 100)


def parse_areas(data):
    def parse_areas_(data, level):
        for item in data:
            id = int(item['id'])
            parent_id = item['parent_id']
            if parent_id is not None:
                parent_id = int(parent_id)
            name = item['name']
            yield Area(id, parent_id, level, name)
            for area in parse_areas_(item['areas'], level + 1):
                yield area
    return parse_areas_(data, 0)


def load_areas():
    with open(AREAS) as file:
        data = json.load(file)
        return parse_areas(data)


def show_age_salary_correlation(resumes):
    x = []
    y = []
    age_salary_sum = Counter()
    age_salary_count = Counter()
    for resume in sample(resumes, 500000):
        age = resume.age
        gender = resume.gender
        if gender is not None and 10 < age < 80 and resume.area_id == 1:
            salary = resume.salary
            if resume.currency == 'RUR' and salary and salary < 150000:
                age_salary_sum[age] += salary
                age_salary_count[age] += 1
                age = age + (random() - 0.5) * 2
                salary = salary + (random() - 0.5) * 3000
                x.append(age)
                y.append(salary)
    fig, ax = plt.subplots()
    ax.scatter(x, y, linewidth=0, color='#4a71b2', alpha=0.01)
    x = []
    y = []
    for age in sorted(age_salary_sum):
        if age > 18:
            mean = float(age_salary_sum[age]) / age_salary_count[age]
            x.append(age)
            y.append(mean)
    ax.plot(x, y, linewidth=1, color='#ff0000')
    ax.set_ylim(10000, 110000)
    ax.set_xlim(10, 65)
    ax.set_xlabel(u'Возраст')
    ax.set_ylabel(u'Ожидаемая зарплата')


def show_gender_salary_correlation(resumes):
    genders = defaultdict(list)
    for resume in sample(resumes, 300000):
        gender = resume.gender
        salary = resume.salary
        if resume.area_id == 1 and gender is not None and salary and salary < 150000:
            genders[gender].append(salary)
    cap = max(len(_) for _ in genders.itervalues())
    for gender, salaries in genders.iteritems():
        size = len(salaries)
        update = [None] * (cap - size)
        salaries.extend(update)
        genders[gender] = salaries
    table = pd.DataFrame(genders)
    fig, ax = plt.subplots()
    table.plot(kind='box', ax=ax)
    ax.set_ylim(0, 110000)
    ax.set_xlabel(u'Пол')
    ax.set_ylabel(u'Ожидаемая зарплата')


def load_school_universities(cap=10):
    universities = Counter()
    with open(SCHOOLS) as file:
        data = json.load(file)
        for school in data:
            top = Counter(school['universities'])
            for university, share in top.most_common(cap):
                universities[university] += 1
    return universities


def encode_university(university):
    university = university.encode('utf8')
    code = base64.b16encode(university)
    return code


def decode_university(code):
    university = base64.b16decode(code)
    university = university.decode('utf8')
    return university


def get_university_filename(university):
    code = encode_university(university)
    return '{code}.json'.format(code=code)


def parse_university_filename(filename):
    code, _ = filename.split('.', 1)
    return decode_university(code)


def list_university_cache():
    for filename in os.listdir(UNIVERSITIES_DIR):
        yield parse_university_filename(filename)


def get_university_path(university):
    filename = get_university_filename(university)
    return os.path.join(UNIVERSITIES_DIR, filename)


def download_university_suggest(university):
    response = requests.get(
        'https://api.hh.ru/suggests/educational_institutions',
        params={
            'text': university
        }
    )
    return response.json()


def dump_university(data, university):
    path = get_university_path(university)
    with open(path, 'w') as file:
        json.dump(data, file)


def load_university(university):
    path = get_university_path(university)
    with open(path) as file:
        return json.load(file)


def load_university_names():
    universities = {}
    table = pd.read_excel(UNIVERSITIES)
    for index, row in table.iterrows():
        label, correct, name = row
        if correct == '+':
            if name in universities and label != universities[name]:
                print >>sys.stderr, u'Labels colide for "{name}"'.format(name=name)
            else:
                universities[name] = label
    return universities
                

def get_specializations(vacancies):
    specializations = {}
    for vacancy in vacancies:
        for specialization in vacancy.specializations:
            specializations[specialization.id] = specialization
    return specializations


def show_gender_specializations(resumes, specializations):
    gender_specializations = defaultdict(Counter)
    for resume in resumes:
        gender = resume.gender
        if gender is not None:
            groups = {specializations[_].group.name for _ in resume.specializations}
            for group in groups:
                gender_specializations[gender][group] += 1
    table = pd.DataFrame({
        0: gender_specializations[0],
        1: gender_specializations[1]
    })
    table = table.div(table.sum(axis=1), axis=0)
    order = table[0].sort_values().index
    table = table.reindex(index=order)
    fig, ax = plt.subplots()
    table.plot(kind='bar', ax=ax)
    ax.set_ylabel(u'Доля пола внутри отрасли')


def show_vacancy_resume_specializations(vacancies, resumes, specializations):
    vacancy_specializations = Counter()
    for vacancy in vacancies:
        groups = {_.group.name for _ in vacancy.specializations}
        for group in groups:
            vacancy_specializations[group] += 1
    resume_specializations = Counter()
    for resume in resumes:
        groups = {specializations[_].group.name for _ in resume.specializations}
        for group in groups:
            resume_specializations[group] += 1
    table = pd.DataFrame({
        'vacancies': vacancy_specializations,
        'resumes': resume_specializations
       })
    table = table.div(table.sum(axis=0), axis=1)
    order = table['resumes'].sort_values(ascending=False).index
    table = table.reindex(index=order)
    fig, ax = plt.subplots()
    table.plot(kind='bar', ax=ax)
    ax.set_ylabel(u'Доля вакансий и доля резюме по отраслям')


def show_vacancy_salary_bounds_distribution(vacancies):
    counts = Counter()
    for vacancy in vacancies:
        salary = vacancy.salary
        if salary is not None:
            counts[salary.min is not None, salary.max is not None] += 1
        else:
            counts[False, False] += 1
    table = pd.Series(counts)
    fig, ax = plt.subplots()
    table.plot(kind='bar', ax=ax)
    ax.set_ylabel(u'Число вакансий')
    ax.set_xticklabels([u'Не указано', u'До', u'От', u'От ... До'])


def show_vacancy_salary_model(vacancies):
    mins = []
    maxes = []
    min_sum = Counter()
    min_count = Counter()
    for vacancy in vacancies:
        salary = vacancy.salary
        if salary:
            min = salary.min
            max = salary.max
            if min is not None and max is not None and max < 150000:
                min_sum[min] += max
                min_count[min] += 1
                min = min + (random() - 0.5) * 3000
                max = max + (random() - 0.5) * 3000
                mins.append(min)
                maxes.append(max)
    fig, ax = plt.subplots()
    ax.scatter(mins, maxes, linewidth=0, color='#4a71b2', alpha=0.01)
    x = []
    y = []
    for min in sorted(min_sum):
        count = min_count[min]
        if count > 100:
            mean = float(min_sum[min]) / count
            x.append(min)
            y.append(mean)
    ax.plot(x, y, linewidth=1, color='#ff0000')
    ax.set_xlim(0, 115000)
    ax.set_ylim(0, None)
    ax.set_xlabel(u'Нижняя граница зарплаты')
    ax.set_ylabel(u'Верхняя граница зарплаты')


def get_mean_salary(salary):
    min = salary.min
    max = salary.max
    if min is None:
        if max < 60000:
            min = max - 10000
        else:
            min = max - 20000
    elif max is None:
        if min < 50000:
            max = min + 10000
        else:
            max = min + 20000
    return float(min + max) / 2


def show_vacancy_resume_salaries(vacancies, resumes, specializations):
    vacancy_salaries_sum = Counter()
    vacancy_salaries_count = Counter()
    for vacancy in vacancies:
        if vacancy.area_id == 1:
            salary = vacancy.salary
            if salary is not None:
                salary = get_mean_salary(salary)
                groups = {_.group.name for _ in vacancy.specializations}
                for group in groups:
                    vacancy_salaries_sum[group] += salary
                    vacancy_salaries_count[group] += 1
    vacancy_salaries = {
        group: float(vacancy_salaries_sum[group]) / vacancy_salaries_count[group]
        for group in vacancy_salaries_sum
    }
    resume_salaries_sum = Counter()
    resume_salaries_count = Counter()
    for resume in resumes:
        age = resume.age
        if resume.area_id == 1 and age and age > 30:
            salary = resume.salary
            if salary is not None and salary < 150000:
                groups = {
                    specializations[_].group.name for _ in resume.specializations
                }
                for group in groups:
                    resume_salaries_sum[group] += salary
                    resume_salaries_count[group] += 1
    resume_salaries = {
        group: float(resume_salaries_sum[group]) / resume_salaries_count[group]
        for group in resume_salaries_sum
    }
    table = pd.DataFrame({
        'resumes': resume_salaries,
        'vacancies': vacancy_salaries
    })
    order = table['vacancies'].sort_values(ascending=False).index
    table = table.reindex(index=order)
    fig, ax = plt.subplots()
    table.plot(kind='bar', ax=ax)
    ax.set_ylabel(u'Зарплата в резюме и в вакансиях')


def get_russian_areas(areas):
    id_areas = {_.id: _ for _ in areas}
    russian_areas = {}
    for area in areas:
        id = area.id
        if area.level == 2:     # Town
            parent = id_areas[area.parent_id]
            if parent.parent_id == 113:  # Russia
                russian_areas[id] = parent
        elif area.level == 1 and area.parent_id == 113: # Msk and Spb
            russian_areas[id] = area
    return russian_areas


def show_geography_salary(resumes, russian_areas):
    areas = defaultdict(list)
    for resume in sample(resumes, 1000000):
        area = russian_areas.get(resume.area_id)
        if area:
            area = area.name
            salary = resume.salary
            if salary and salary < 150000:
                areas[area].append(salary)
    cap = max(len(_) for _ in areas.itervalues())
    for area, salaries in areas.iteritems():
        size = len(salaries)
        update = [None] * (cap - size)
        salaries.extend(update)
        areas[area] = salaries
    table = pd.DataFrame(areas)
    order = table.mean(axis=0) - table.std(axis=0)
    order = order.sort_values(ascending=False).index
    order = order[:30]
    table = table.reindex(columns=order)
    fig, ax = plt.subplots()
    table.plot(kind='box', ax=ax)
    ax.set_ylim(0, 115000)
    ax.set_xticklabels(order, rotation=90)


def show_university_salary(resumes, university_names):
    universities = defaultdict(list)
    for resume in resumes:
        age = resume.age
        if age and age > 25 and resume.area_id == 1:
            for education in resume.educations:
                university = university_names.get(education)
                if university:
                    salary = resume.salary
                    if salary and salary < 150000:
                        universities[university].append(salary)
    cap = max(len(_) for _ in universities.itervalues())
    for university, salaries in universities.iteritems():
        size = len(salaries)
        update = [None] * (cap - size)
        salaries.extend(update)
        universities[university] = salaries
    table = pd.DataFrame(universities)
    order = table.mean(axis=0)
    order = order.sort_values(ascending=False).index
    order = order[:30]
    table = table.reindex(columns=order)
    fig, ax = plt.subplots()
    table.plot(kind='box', ax=ax)
    ax.set_ylim(0, 115000)
    ax.set_xticklabels(order, rotation=90)


def shorten_string(string, cap=20):
    if len(string) <= cap:
        return string
    else:
        return string[:cap] + '...'


def show_geography_specializations(resumes, russian_areas, specializations):
    geography_specializations = defaultdict(Counter)
    for resume in sample(resumes, 100000):
        area = russian_areas.get(resume.area_id)
        if area:
            area = area.name
            groups = {specializations[_].group.name for _ in resume.specializations}
            for group in groups:
                group = shorten_string(group)
                geography_specializations[area][group] += 1
    total = Counter()
    order = Counter()
    for area in geography_specializations:
        groups = geography_specializations[area]
        total += groups
        order[area] = sum(groups.itervalues())
    order = [area for area, _ in order.most_common()]
    top = [specialization for specialization, _ in total.most_common(10)]
    total = pd.Series(total)
    total = total.reindex(top)
    total = total / total.sum()
    fig, axis = plt.subplots(5, 3)
    fig.set_size_inches(18, 10)
    for area, ax in zip(order, axis.flatten()):
        table = pd.Series(geography_specializations[area])
        table = table.reindex(index=top)
        table = table / table.sum()
        table = table / total - 1
        table.plot(kind='barh', ax=ax)
        ax.set_title(area)
        ax.set_xlim(-1, 1)
    fig.tight_layout()
