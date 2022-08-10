# {
#     version: 1.0,
#     name: Eryc Masselli, 22/08
#     description: Extract and Load "EL.pokemon_growth_rates" table to bigquery
# }

import config as cf

if __name__ == "__main__":
    all_growth_rates = ['slow', 'medium', 'fast', 'medium-slow', 'slow-then-very-fast', 'fast-then-very-slow']
    pokemon_growth_rates = cf.pd.DataFrame(columns=['pokemon_name', 'Growth_id', 'growth_name'])
    growth_rates = cf.pd.DataFrame()

    for rate in all_growth_rates:
        growth_rate = cf.pb.growth_rate(rate)
        levels = []
        for level in growth_rate.levels:
            levels.append(level.experience)

        for pokemon in growth_rate.pokemon_species:
            row = {
                'pokemon_name': pokemon.name,
                'Growth_id': growth_rate.id,
                'growth_name': growth_rate.name
            }
            pokemon_growth_rates = pokemon_growth_rates.append(row, ignore_index=True)
        growth_rates[rate] = levels

        cf.time.sleep(3)
    
    job_config = cf.bigquery.LoadJobConfig(
        autodetect=True
    )

    growth_rates.columns =['slow', 'medium', 'fast', 'medium_slow', 'slow_then_very_fast', 'fast_then_very_slow']

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'pokemon_growth_rates'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    cf.client.load_table_from_dataframe(pokemon_growth_rates,table_id, job_config=job_config)

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'growth_rates'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    cf.client.load_table_from_dataframe(growth_rates,table_id, job_config=job_config)