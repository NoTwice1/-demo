# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.http import HttpResponse
from django.template import RequestContext

from django.shortcuts import render

import requests
from Queue import Queue
import urllib
try:
    import simplejson as json
except:
    import json
from collections import defaultdict
import jieba
import re
import cPickle

import build_dict

attr_map = build_dict.load_attr_map("/mnt/demo/search/data/attr_mapping.txt")
attr_ac = cPickle.load(open("/mnt/demo/search/data/attr_ac.pkl","rb"))
ent_dict = build_dict.load_entity_dict("/mnt/demo/search/data/all_entity.txt")

def home(request):
    return render(request, "home.html", {})

def search(request):
    question = request.GET['question']
    answer, msg, query_type = _parse_query(question)
    if msg == 'done':
        if query_type == 1:
            return render(request, "entity.html", {"question":question, "ans":answer})
        elif query_type == 4:
            return render(request, "entity_list.html", {"question":question, "ans":answer})
        elif query_type == 3:
            if isinstance(answer, int):
                answer = str(answer)
            return render(request, "message.html", {"question":question, "ans":answer})
    elif msg == 'none':
        return render(request, "message.html", {"question":question, "ans":"find nothing"})
    else:
        return render(request, "message.html", {"question":question, "ans":answer + " " + msg})

def _parse_query(question):
    answer, query_type = "", None
    question = question.upper()
    question = question.replace(" ","")
    parts = re.split("：|:|<|>|<=|>=", question)
    en = _entity_linking(parts[0])
    if len(parts) < 2:
        if len(en):
            query_type = 1
            answer,msg = _search_single_subj(en[-1])
        else:
            return question, '未识别到实体',-1
    elif 'AND' in question or 'OR' in question:
        query_type = 4
        bool_ops = re.findall('AND|OR',question)
        exps = re.split('AND|OR',question)        
        answer,msg = _search_multi_PO(exps, bool_ops)
        # answer = '#'.join(answer)
    elif len(_map_predicate(parts[0])) != 0:
        query_type = 4
        answer, msg = _search_multi_PO([question],[])    
    elif len(en):
        query_type = 3
        answer, msg = _search_multihop_SP(parts)
    else:
        msg = '未识别到实体或属性: ' + parts[0]

    return answer, msg, query_type

def _search_multihop_SP(parts):
    has_done = parts[0]
    v = parts[0]
    for i in range(1, len(parts)):
        en = _entity_linking(v)
        if not len(en):
            return '执行到: ' + has_done, '==> 对应的结果为:' + v + ', 知识库中没有该实体: ' + v
        card, msg = _search_single_subj(en[-1])
        p = _map_predicate(parts[i])
        if not len(p):
            return '执行到: ' + has_done, '==> 知识库中没有该属性: ' + parts[i]
        p = p[0]
        if p not in card:
            return '执行到: ' + has_done, '==> 实体 ' + card['subj'] + ' 没有属性 ' + p
        v = card[p]
        if isinstance(v,int):
            v = str(v)
        has_done += ":" + parts[i]
    return v, 'done'

def _search_multi_PO(exps, bool_ops):
    ans_list = []
    po_list = []
    cmp_dir = {
        "<":"lt",
        "<=":"lte",
        ">":"gt",
        ">=":"gte"
    }

    for e in exps:
        if e == "":
            return "", 'AND 或 OR 后不能为空'

        begin_with_NOT = False
        if e[0:3] == 'NOT':
            begin_with_NOT = True
            e = e[3:]
        elif 'NOT' in e:
            return e, 'NOT请放在PO对前面'

        op = re.findall("：|:|>|<|>=|<=",e)
        if len(op) != 1:
            return e, '语法错误'
        op = op[0]
        if op == '<' or op == '>':
            index = e.find(op)
            if e[index+1] == '=':
                op = op + '='
        pred, obj = e.split(op)
        c_pred = _map_predicate(pred)
        if not len(c_pred):
            return e, '知识库中没有该属性: ' + pred
        if obj == '':
            return e+"?", '属性值不能为空'
        pred = c_pred[0]
        
        part_query = ""
        if not begin_with_NOT:
            if op == ':' or op == '：':
                if pred == 'height' or pred == 'weight':
                    part_query = '{"term":{"' + pred + '":' + obj + '}}'
                else:
                    part_query = '{"nested":{"path":"po","query":{"bool":{"must":[{"term":{"po.pred":"' + pred + \
                            '"}},{"term":{"po.obj":"' + obj + '"}}]}}}}'
            else:
                if pred == 'height' or pred == 'weight':
                    part_query = '{"range":{"' + pred + '":{"' + cmp_dir[op] + '":' + obj + '}}}'
                else:
                    return e,'该属性不支持比较大小,目前只支持height,weight'
        else:
            if op == ':' or op == '：':
                if pred == 'height' or pred == 'weight':
                    part_query = '{"bool":{"must_not":{"term":{"' + pred + '":' + obj + '}}}}'
                else:
                    part_query = '{"nested":{"path":"po","query":{"bool":{"must":[{"term":{"po.pred":"' + pred + \
                            '"}},{"bool":{"must_not":{"term":{"po.obj":"' + obj + '"}}}}]}}}}'
            else:
                if pred == 'height' or pred == 'weight':
                    part_query = '{"bool":{"must_not":{"range":{"' + pred + '":{"' + cmp_dir[op] + '":' + obj + \
                            '}}}}}'
                else:
                    return e,'该属性不支持比较大小,目前只支持height,weight'        
        po_list.append(part_query)

    or_po = [False] * len(exps)
    should_list = []
    must_list = []
    i = 0
    while i < len(bool_ops):
        if bool_ops[i] == 'OR':
            adjacent_or = [po_list[i]]
            or_po[i] = True
            while i < len(bool_ops) and bool_ops[i] == 'OR':
                adjacent_or.append(po_list[i+1])
                or_po[i+1] = True
                i += 1
            should_list.append(",".join(adjacent_or))
        i += 1
    for i,po in enumerate(or_po):
        if not po:
            must_list.append(po_list[i])
    must_list = ",".join(must_list)
    query = ""
    if must_list:
        query = '{"query":{"bool":{"must":[' + must_list + ']'
        if should_list:
            query += ","
            for s in should_list:
                query += '"should":[' + s + '],'
            query = query[:-1]
        query += '}}}' 
    else:
        query = '{"query":{"bool":{'
        if should_list:
            for s in should_list:
                query += '"should":[' + s + '],'
            query = query[:-1]
        query += '}}}'
    
    query = query.encode('utf-8')
    response = requests.get("http://localhost:9200/demo/person/_search", data = query)
    res = json.loads(response.content)

    if res['hits']['total'] == 0:
        return None,'none'
    else:
        ans = {}
        for e in res['hits']['hits']:
            name = e['_source']['subj']
            ans[name] = "/search?question="+name 

        return ans, 'done'
        # return query.decode('utf-8'), 'done'


def _search_single_subj(entity_name):
    query = json.dumps({"query": { "bool":{"filter":{"term" :{"subj" : entity_name}}}}})
    response = requests.get("http://localhost:9200/demo/person/_search", data = query)
    res = json.loads(response.content)

    if res['hits']['total'] == 0:
        return None, 'entity'
    else:
        card = dict()
        card['subj'] = entity_name
        s = res['hits']['hits'][0]['_source']
        if 'height' in s:
            card['height'] = s['height']
        if 'weight' in s:
            card['weight'] = s['weight']
        for po in s['po']:
            if po['pred'] in card:
                card[po['pred']] += ' ' + po['obj']
            else:
                card[po['pred']] = po['obj']
        return card, 'done'

def _search_single_subj_pred_pair(entity_name, attr_name):
    query = '{"query": {"constant_score": {"filter": {"bool": {"must": {"term": {"pred": "' + \
        attr_name + '"}},"must":{"term":{"subj":"' + entity_name + '"}}}}}}}'
    query = query.encode('utf-8')
    response = requests.get("http://localhost:9200/demo/person/_search", data = query)
    res = json.loads(response.content)

    if res['hits']['total'] == 0:
        ans, _ = _search_single_subj(entity_name)
        return ans, 'str'
    else:
        obj = res['hits']['hits'][0]['_source']['obj']
        # obj_en, _ = _search_single_subj(obj)
        # if obj_en is not None:
        #     return obj_en, 'entity'
        # else:
        return obj, 'str'

def _map_predicate(pred_name):

    def _map_attr(word):
        ans = []
        for w in attr_map[word.encode('utf-8')]:
            ans.append(w.decode('utf-8'))
        return ans

    match = []
    for w in attr_ac.iter(pred_name.encode('utf-8')):
        match.append(w[1][1].decode('utf-8'))
    if not len(match):
        return []

    ans = _map_attr(match[0])
    return ans

def _entity_linking(entity_name):

    def _generate_ngram_word(word_list_gen):
        word_list = []
        for w in word_list_gen:
            word_list.append(w)
        n = len(word_list)
        ans = []
        for i in range(1, n+1):
            for j in range(0,n+1-i):
                ans.append(''.join(word_list[j:j+i]))
        return ans

    parts = re.split(r'的|是|有', entity_name)
    ans = []
    ans1 = ""
    for p in parts:
        pp = jieba.cut(p)
        if pp is not None:
            for phrase in _generate_ngram_word(pp):
                if phrase.encode('utf-8') in ent_dict:
                    ans.append(phrase)
    return ans
