select
  pk.type1,
  count(distinct pk.id) type1_pokemon_counts,
  pk2.type2,
  pk2.type2_pokemon_counts
from {{ source('EL', 'pokemons') }} pk
left join 
  (
    select
      type2,
      count(distinct id)  type2_pokemon_counts
    from {{ source('EL', 'pokemons') }}
    group by 1
  ) pk2 on pk.type1 = pk2.type2
group by 1,3,4
order by type1_pokemon_counts DESC, type2_pokemon_counts DESC