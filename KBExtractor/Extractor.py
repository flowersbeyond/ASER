from aser.database.db_API import KG_Connection
from aser.database.db_API import generate_event_id, generate_relation_id, generate_id
#We provide three different modes: insert, cache, and memory.

#memory mode loads the whole data in the memory
#However, it still takes a lot of time, and costs a lot memory due to the large size. We use the cache mode to build a DB connection.
#kg_conn = KG_Connection(db_path=r'data/database/core/KG_v0.1.0.db', mode='cache')
kg_conn = KG_Connection(db_path=r'data/database/core/KG_v0.1.0.db', mode='memory')

event_id_set_file = r'data/database/eventid.txt'
rel_id_set_file = r'data/database/relid.txt'
print('SIZE:', 'eventualities: ', len(kg_conn.event_id_set), 'relations:', len(kg_conn.relation_id_set))
'''
with open(event_id_set_file, mode='w',encoding='utf-8') as fout:
    for event_id in kg_conn.event_id_set:
        fout.write(event_id)
        fout.write('\n')
with open(rel_id_set_file, mode='w', encoding='utf-8') as fout:
    for rel_id in kg_conn.relation_id_set:
        fout.write(rel_id)
        fout.write('\n')
'''

event_list = kg_conn.get_all_events()
event_dict = {}
for event in event_list:
    event_dict[event['_id']] = event

rel_list = kg_conn.get_all_relations()
count = 0
for relation in rel_list:
    e1 = event_dict[relation['event1_id']]
    e2 = event_dict[relation['event2_id']]
    e1_text = e1['words']
    e2_text = e2['words']
    relation['e1_text'] = e1_text
    relation['e2_text'] = e2_text
    count += 1
    if count >= 100:
        break


all_relation_file = r'data/database/all_relation_text.txt'
with open(all_relation_file, mode='w', encoding='utf-8') as fout:
    for rel in rel_list:
        fout.write(str(rel))
        fout.write('\n')

#SIZE: eventualities:  27565673 relations: 8834257
print(list(zip(kg_conn.event_columns, kg_conn.event_column_types)))
#[('_id', 'PRIMARY KEY'), ('verbs', 'TEXT'), ('skeleton_words_clean', 'TEXT'), ('skeleton_words', 'TEXT'), ('words', 'TEXT'), ('pattern', 'TEXT'), ('frequency', 'REAL')]
print(list(zip(kg_conn.relation_columns, kg_conn.relation_column_types)))
#[('_id', 'PRIMARY KEY'), ('event1_id', 'TEXT'), ('event2_id', 'TEXT'), ('Precedence', 'REAL'), ('Succession', 'REAL'), ('Synchronous', 'REAL'), ('Reason', 'REAL'), ('Result', 'REAL'), ('Condition', 'REAL'), ('Contrast', 'REAL'), ('Concession', 'REAL'), ('Conjunction', 'REAL'), ('Instantiation', 'REAL'), ('Restatement', 'REAL'), ('ChosenAlternative', 'REAL'), ('Alternative', 'REAL'), ('Exception', 'REAL'), ('Co_Occurrence', 'REAL')]

#kg_conn.get_exact_match_event(generate_id('i learn python'))
#kg_conn.get_exact_match_relation([generate_id('i be tired'), generate_id('i sleep')])
