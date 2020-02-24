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
    return str(event_list)

def pretty_rel_str(rel_list):
    return str(rel_list)

def get_event_id(kg_conn, query, event_list_cache):
    return generate_id(query)

def main():
    kg_conn = KG_Connection(db_path=r'data/database/core/KG_v0.1.0.db', mode='memory')
    event_list_cache = {}
    while True:
        query = input(__prompt='command:')
        if query.startswith('E:'):
            query = query[2:].strip()
            event_list_cache = get_match_events(kg_conn, query)
            print(pretty_event_str(event_list_cache))
        if query.startswith('R:'):
            query = query[2:].strip()
            event_id = get_event_id(kg_conn, query, event_list_cache)
            rel_list = get_event_all_rels(kg_conn, event_id)
            print(pretty_rel_str(rel_list))
        if query.startswith('Q:'):
            return

if __name__ == '__main__':
    main()


#SIZE: eventualities:  27565673 relations: 8834257
#print(list(zip(kg_conn.event_columns, kg_conn.event_column_types)))
#[('_id', 'PRIMARY KEY'), ('verbs', 'TEXT'), ('skeleton_words_clean', 'TEXT'), ('skeleton_words', 'TEXT'), ('words', 'TEXT'), ('pattern', 'TEXT'), ('frequency', 'REAL')]
#print(list(zip(kg_conn.relation_columns, kg_conn.relation_column_types)))
#[('_id', 'PRIMARY KEY'), ('event1_id', 'TEXT'), ('event2_id', 'TEXT'), ('Precedence', 'REAL'), ('Succession', 'REAL'), ('Synchronous', 'REAL'), ('Reason', 'REAL'), ('Result', 'REAL'), ('Condition', 'REAL'), ('Contrast', 'REAL'), ('Concession', 'REAL'), ('Conjunction', 'REAL'), ('Instantiation', 'REAL'), ('Restatement', 'REAL'), ('ChosenAlternative', 'REAL'), ('Alternative', 'REAL'), ('Exception', 'REAL'), ('Co_Occurrence', 'REAL')]

#kg_conn.get_exact_match_event(generate_id('i learn python'))
#kg_conn.get_exact_match_relation([generate_id('i be tired'), generate_id('i sleep')])
