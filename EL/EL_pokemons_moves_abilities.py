# {
#     version: 1.0,
#     name: Eryc Masselli, 22/08
#     description: Extract and Load "EL.pokemons","EL.pokemon_moves","EL.pokemon_abilities","EL.abilities" table to bigquery
# }


import config as cf

if __name__ == "__main__":

    pokemons = cf.pd.DataFrame(columns=['id', 'name', 'base_experience', 'height', 'weight', 'order', 'is_default', 'type1', 'type2', 'hp_effort', 'attack_effort', 'defense_effort', 'special_attack_effort', 'special_defense_effort', 'speed_effort', 'hp_base_stat', 'attack_base_stat', 'defense_base_stat', 'special_attack_base_stat', 'special_defense_base_stat', 'speed_base_stat'])
    pokemon_moves = cf.pd.DataFrame(columns=['pokemon_id', 'pokemon_name', 'move_name'])
    pokemon_abilities = cf.pd.DataFrame(columns=['pokemon_id', 'pokemon_name', 'is_hidden', 'ability_name'])
    abilities = cf.pd.DataFrame(columns=['id', 'name', 'is_main_series', 'generation', 'effect', 'short_effect', 'resume_effect'])

    for label in cf.generation.pokemon_species:
        pokemon = cf.pb.pokemon(label.name)
        row = {'id': pokemon.id, 
            'name': pokemon.name, 
            'base_experience': pokemon.base_experience, 
            'height': pokemon.height, 
            'weight': pokemon.weight,
            'order': pokemon.order,
            'is_default': pokemon.is_default, 
            'type1': cf.try_except(lambda: pokemon.types[0].type.name, ''), 
            'type2': cf.try_except(lambda: pokemon.types[1].type.name, ''), 
            'hp_effort':cf.try_except(lambda: pokemon.stats[0].effort,''), 
            'attack_effort':cf.try_except(lambda: pokemon.stats[1].effort,''), 
            'defense_effort':cf.try_except(lambda: pokemon.stats[2].effort,''), 
            'special_attack_effort':cf.try_except(lambda: pokemon.stats[3].effort,''), 
            'special_defense_effort':cf.try_except(lambda: pokemon.stats[4].effort,''), 
            'speed_effort':cf.try_except(lambda: pokemon.stats[5].effort,''), 
            'hp_base_stat':cf.try_except(lambda: pokemon.stats[0].base_stat,''), 
            'attack_base_stat':cf.try_except(lambda: pokemon.stats[1].base_stat,''), 
            'defense_base_stat':cf.try_except(lambda: pokemon.stats[2].base_stat,''), 
            'special_attack_base_stat':cf.try_except(lambda: pokemon.stats[3].base_stat,''), 
            'special_defense_base_stat':cf.try_except(lambda: pokemon.stats[4].base_stat,''), 
            'speed_base_stat':cf.try_except(lambda: pokemon.stats[5].base_stat,'') }
        pokemons = pokemons.append(row, ignore_index=True)

        for value in pokemon.moves:
            row = {
                'pokemon_id': pokemon.id,
                'pokemon_name': pokemon.name,
                'move_name': value.move.name}
            pokemon_moves = pokemon_moves.append(row, ignore_index=True)
        
        for value in pokemon.abilities:
            row = {
                'pokemon_id': pokemon.id,
                'pokemon_name': pokemon.name,
                'is_hidden': value.is_hidden,
                'ability_name': value.ability.name}
            pokemon_abilities = pokemon_abilities.append(row, ignore_index=True)
        
        cf.time.sleep(3)

    job_config = cf.bigquery.LoadJobConfig(
        autodetect=True
    )

    for ability_name in pokemon_abilities.ability_name:
    
        effect =''
        short_effect = ''
        flavor_text =''

        ability = cf.pb.ability(ability_name)

        for ef in ability.effect_entries:
            if ef.language.name == 'en':
                effect = ef.effect
                short_effect = ef.short_effect

        for fl in ability.flavor_text_entries:
            if fl.language.name == 'en':
                flavor_text = fl.flavor_text

        row = {
            'id': pokemon.id,
            'name': pokemon.name,
            'is_main_series': value.is_hidden,
            'generation': value.ability.name,
            'effect':effect,
            'short_effect':short_effect,
            'resume_effect':flavor_text}
        
        abilities = abilities.append(row, ignore_index=True)

        cf.time.sleep(3)

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'pokemons'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    cf.client.load_table_from_dataframe(pokemons,table_id, job_config=job_config)

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'pokemon_moves'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    cf.client.load_table_from_dataframe(pokemon_moves,table_id, job_config=job_config)

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'pokemon_abilities'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    cf.client.load_table_from_dataframe(pokemon_abilities,table_id, job_config=job_config)

    bigquery_name = 'pokemondw'
    table_dataset = 'EL'
    table_name = 'abilities'
    table_id = bigquery_name + "." + table_dataset + "." + table_name
    client.load_table_from_dataframe(abilities,table_id, job_config=job_config)