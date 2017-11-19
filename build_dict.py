import ahocorasick
import cPickle
from collections import defaultdict

attr_list_file = './data/attr_mapping.txt'
attr_out_path = './data/attr_ac.pkl'

def dump_ac_attr_dict(attr_mapping_file, out_path):
    '''
    使用AC自动机判断一个字符串中是否包含数据集中的属性名，可以换成字典匹配
    '''
    A = ahocorasick.Automaton()
    f = open(attr_mapping_file)
    i = 0
    for line in f:
        parts = line.strip().split(" ")
        for p in parts:
            if p != "":
                A.add_word(p,(i,p))
                i += 1
    A.make_automaton()
    cPickle.dump(A,open(out_path,'wb'))

def load_ac_dict(out_path):
    '''
    加载创建好的AC自动机对象
    '''
    A = cPickle.load(open(out_path,"rb"))
    return A

def load_attr_map(attr_mapping_file):
    '''
    读取属性的同义词词典，在解析用户查询时将属性词映射为存在于数据集中的同一属性
    '''
    f = open(attr_mapping_file)
    mapping = defaultdict(list)
    for line in f:
        parts = line.strip().split(" ")
        for p in parts:
            if p != '':
                mapping[p].append(parts[0])
    return mapping

def load_entity_dict(entity_file):
    '''
    读取实体名称的词典，用于快速判断一个字符串中是否包含存在于数据集中的实体名。
    如果数据集较大，这一步可以替换为用检索elasticsearch实现
    '''
    f = open(entity_file)
    ents = {}
    for line in f:
        ents[line.strip()] = 1
    return ents

if __name__ == '__main__':
    dump_ac_attr_dict(attr_list_file, attr_out_path)
