import datetime
from aser.database.db_API import KG_Connection
from aser.database.db_API import generate_event_id, generate_relation_id, generate_id
#We provide three different modes: insert, cache, and memory.

#memory mode loads the whole data in the memory
#However, it still takes a lot of time, and costs a lot memory due to the large size. We use the cache mode to build a DB connection.
#kg_conn = KG_Connection(db_path=r'data/database/core/KG_v0.1.0.db', mode='cache')



def get_match_events(kg_conn, query):
    return kg_conn.get_event_by_substring(query)


def get_event_all_rels(kg_conn, event_id):
    return kg_conn.get_relations_by_event_id(event_id)


def pretty_event_str(event_list):
    pretty_str = ''
    for event in event_list:
        pretty_str += 'VERBS: ' + event['verbs'] + '\t' + 'TEXT: ' + event['words'] + '\n'

    return pretty_str


def pretty_rel_str(event_words, rel_list):
    rel_names = ['Precedence','Succession','Synchronous','Reason', 'Result', 'Condition', 'Contrast', 'Concession',
                 'Conjunction', 'Instantiation', 'Restatement', 'ChosenAlternative', 'Alternative', 'Exception',
                 #'Co_Occurrence'
                 ]
    pretty_str = ''
    for rel in rel_list:
        event1_words = event_words
        event2_words = event_words
        if 'event1_words' in rel:
            event1_words = rel['event1_words']
        if 'event2_words' in rel:
            event2_words = rel['event2_words']


        raw_rel_str = ''
        for rel_name in rel_names:
            if rel[rel_name] >= 0.01:
                raw_rel_str += '%s:%f' % (rel_name, rel[rel_name])
        if raw_rel_str != '':
            pretty_str += 'E1: %s\tE2: %s\t REL: %s\n' % (event1_words, event2_words, raw_rel_str)

    return pretty_str


def get_event_id(kg_conn, query):#, event_list_cache):
    #for event in event_list_cache:
    #    if event['words'] == query:
    #        return event['_id']
    return generate_id(query)


def execute_query(query, kg_conn):
    if query.startswith('E:'):
        query = query[2:].strip()
        event_list_cache = get_match_events(kg_conn, query)
        if len(event_list_cache) == 0:
            return "event not found"
        return pretty_event_str(event_list_cache)


    if query.startswith('R:'):
        query = query[2:].strip()
        event_id = get_event_id(kg_conn, query)#, event_list_cache)
        rel_list = get_event_all_rels(kg_conn, event_id)
        return pretty_rel_str(query, rel_list)

    if query.startswith('C:'):
        query = query[2:].strip()
        event_list = get_match_events(kg_conn, query)
        all_rel_list = ''
        for event in event_list:
            rel_list = get_event_all_rels(kg_conn, event['_id'])
            rel_list_str = pretty_rel_str(event['words'], rel_list)
            if rel_list_str != '':
                print(rel_list_str)
            all_rel_list += rel_list_str
        return all_rel_list

    return ''


def connect_db(db_path=r'data/database/core/KG_v0.1.0.db', mode='cache'):
    print(datetime.datetime.now().strftime("[%H:%M:%S]"))
    kg_conn = KG_Connection(db_path=db_path, mode=mode)
    print(datetime.datetime.now().strftime("[%H:%M:%S]"))
    return kg_conn

def interactive_main():

    kg_conn = connect_db()

    event_list_cache = {}
    while True:
        query = input('command:')

        if query.startswith('Q:'):
            return
        else:
            result = execute_query(query, kg_conn)
            print(result)

def batch_main(q_file, result_file):
    kg_conn = connect_db()

    with open(q_file, encoding='utf-8') as fin, open(result_file, mode='w', encoding='utf-8')as fout:
        for l in fin:
            result = execute_query('C:' + l, kg_conn)
            fout.write('query:%s\n' % l)
            fout.write(result)
            fout.write('\n\n')


if __name__ == '__main__':
    event_list_file = r'data/casestudy_manual_event.txt'
    result_file = r'data/casestudy_manual_event_all_rels.txt'
    #batch_main(event_list_file, result_file)
    interactive_main()


#SIZE: eventualities:  27565673 relations: 8834257
#print(list(zip(kg_conn.event_columns, kg_conn.event_column_types)))
#[('_id', 'PRIMARY KEY'), ('verbs', 'TEXT'), ('skeleton_words_clean', 'TEXT'), ('skeleton_words', 'TEXT'), ('words', 'TEXT'), ('pattern', 'TEXT'), ('frequency', 'REAL')]
#print(list(zip(kg_conn.relation_columns, kg_conn.relation_column_types)))
#[('_id', 'PRIMARY KEY'), ('event1_id', 'TEXT'), ('event2_id', 'TEXT'), ('Precedence', 'REAL'), ('Succession', 'REAL'), ('Synchronous', 'REAL'), ('Reason', 'REAL'), ('Result', 'REAL'), ('Condition', 'REAL'), ('Contrast', 'REAL'), ('Concession', 'REAL'), ('Conjunction', 'REAL'), ('Instantiation', 'REAL'), ('Restatement', 'REAL'), ('ChosenAlternative', 'REAL'), ('Alternative', 'REAL'), ('Exception', 'REAL'), ('Co_Occurrence', 'REAL')]

#kg_conn.get_exact_match_event(generate_id('i learn python'))
#kg_conn.get_exact_match_relation([generate_id('i be tired'), generate_id('i sleep')])
