# {
#     version: 1.0,
#     name: Eryc Masselli, 22/08
#     description: Extract and Load "EL.moves" table to bigquery
# }

import config as cf

if __name__ == "__main__":

    generation = cf.pb.generation(1)
    moves = cf.pd.DataFrame(columns=['id', 'name', 'accuracy', 'pp', 'priority', 'power', 'damage_class', 'type', 'use_before1', 'use_before2', 'use_before3', 'use_after1', 'use_after2', 'use_after3'])
    
    for value in generation.moves:
        move = cf.pb.move(value.name)

        row={'id':move.id, 
            'name':move.name, 
            'accuracy':move.accuracy, 
            'pp':move.pp, 
            'priority':move.priority, 
            'power':move.power, 
            'damage_class':move.damage_class.name, 
            'type':move.type.name,
            'use_before1': cf.try_except(lambda: move.contest_combos.normal.use_before[0].name,''), 
            'use_before2': cf.try_except(lambda: move.contest_combos.normal.use_before[1].name,''), 
            'use_before3': cf.try_except(lambda: move.contest_combos.normal.use_before[2].name,''), 
            'use_after1': cf.try_except(lambda: move.contest_combos.normal.use_after[0].name,''), 
            'use_after2': cf.try_except(lambda: move.contest_combos.normal.use_after[1].name,''), 
            'use_after3': cf.try_except(lambda: move.contest_combos.normal.use_after[2].name,'')}
        moves = moves.append(row, ignore_index=True)
        cf.time.sleep(3)


    job_config = cf.bigquery.LoadJobConfig(
        autodetect=True
    )

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'moves'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    cf.client.load_table_from_dataframe(moves,table_id, job_config=job_config)