# {
#     version: 1.0,
#     name: Eryc Masselli, 22/08
#     description: Extract and Load "EL.types" table to bigquery
# }

import config as cf

if __name__ == "__main__":
    types = ['normal', 'fighting', 'flying', 'poison', 'ground', 'rock', 'bug','ghost','steel','fire','water','grass','electric','psychic','ice','dragon','dark','fairy','unknown', 'shadow']

    json_type = []

    for name in types:

        type = cf.pb.type_(name)
        types_damage_list = {}
        
        types_arr = []
        for t in type.damage_relations.no_damage_to:
            types_arr.append({"type":t.name})
        types_damage_list['no_damage_to'] = types_arr

        types_arr = []
        for t in type.damage_relations.half_damage_to:
            types_arr.append({"type":t.name})
        types_damage_list['half_damage_to'] = types_arr

        types_arr = []
        for t in type.damage_relations.double_damage_to:
            types_arr.append({"type":t.name})
        types_damage_list['double_damage_to'] = types_arr

        types_arr = []
        for t in type.damage_relations.no_damage_from:
            types_arr.append({"type":t.name})
        types_damage_list['no_damage_from'] = types_arr

        types_arr = []
        for t in type.damage_relations.half_damage_from:
            types_arr.append({"type":t.name})
        types_damage_list['half_damage_from'] = types_arr

        types_arr = []
        for t in type.damage_relations.double_damage_from:
            types_arr.append({"type":t.name})
        types_damage_list['double_damage_from'] = types_arr

        types_damage_list['type'] = name
        json_type.append(types_damage_list)


    
    job_config = cf.bigquery.LoadJobConfig(
        schema=[
            cf.bigquery.SchemaField("type", "STRING",mode="REQUIRED"),
            cf.bigquery.SchemaField(
                "no_damage_to",
                "RECORD",
                mode="REPEATED",
                fields=[
                    cf.bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ],
            ),
            cf.bigquery.SchemaField(
                "half_damage_to",
                "RECORD",
                mode="REPEATED",
                fields=[
                    cf.bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ],
            ),
            cf.bigquery.SchemaField(
                "double_damage_to",
                "RECORD",
                mode="REPEATED",
                fields=[
                    cf.bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ],
            ),
            cf.bigquery.SchemaField(
                "no_damage_from",
                "RECORD",
                mode="REPEATED",
                fields=[
                    cf.bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ],
            ),
            cf.bigquery.SchemaField(
                "half_damage_from",
                "RECORD",
                mode="REPEATED",
                fields=[
                    cf.bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ],
            ),
            cf.bigquery.SchemaField(
                "double_damage_from",
                "RECORD",
                mode="REPEATED",
                fields=[
                    cf.bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ],
            ),
            
        ],
    )

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'types'
    table_id = bigquery_name + "." + table_dataset + "." + table_name

    cf.client.load_table_from_json(json_type, table_id, job_config=job_config)