#coding:utf-8
'''
实验数据集的预处理和格式转换
一个实体及其所关联的所有属性和属性值转化为一个json对象，作为将导入elasticsearch的一个文档
'''
try:
	import simplejson as json
except:
	import json
import os
import re

def transform_entity2json(input_file):
	'''
	一个entity的所有属性为一个文档
	height,weight由于要支持range搜索，需要另存为int类型，要单独考虑
	'''
	dirname = os.path.dirname(input_file)
	basename = os.path.basename(input_file)
	out_name = basename[:basename.rfind(".")]

	f_input = open(input_file)
	f_json = open(dirname + "/" + out_name + ".json","w")

	last = None
	new_ent = {'po':[]}
	for line in f_input:
		parts = line.strip().split(" ")
		entity = parts[0]
		attr = parts[1]
		attr_vals = " ".join(parts[2:])
		if last is None:
			last = entity		

		if last is not None and entity != last:
			new_ent['subj'] = last
			new_ent_j = json.dumps(new_ent)
			f_json.write(new_ent_j + "\n")
			last = entity
			new_ent = {}
			new_ent['po'] = []

		if attr == 'height':
			v = clean_height(attr_vals)
			if v is not None:
				new_ent['height'] = v
		elif attr == 'weight':
			v = clean_weight(attr_vals)
			if v is not None:
				new_ent['weight'] = v
		elif attr != 'description':
			v = clean_normal(attr_vals)
			for vv in v:
				new_ent['po'].append({'pred':attr,'obj':vv})
		else:
			new_ent['po'].append({'pred':"description","obj":attr_vals})

	new_ent['subj'] = last
	new_ent_j = json.dumps(new_ent)
	f_json.write(new_ent_j + "\n")

def clean_height(h):
	cm = re.findall('\d{3}',h)
	if len(cm):
		return int(cm[0])
	m = re.findall('\d\.\d{2,3}',h)
	if len(m):
		return int(float(m[0]) * 100)
	return None

def clean_weight(w):
	w = w.replace(" ","")
	kg = re.findall('\d{2,3}\.?\d?',w)
	if len(kg):
		return int(float(kg[0]))
	return None

def clean_normal(attr_vals):
	v = []
	a = re.split(" |,|，|、|\|/|#|;|；", attr_vals)
	for aa in a:
		if aa:
			v.append(aa)
	return v			

if __name__ == '__main__':	
	transform_entity2json("../data/Person.txt")