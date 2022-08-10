WITH pokemon_type_rank AS (
    SELECT 
      CAST(ROW_NUMBER() OVER(PARTITION BY type1 ORDER BY total DESC) AS int64) pokemon_type_rank, 
      *
    FROM {{ ref('top_base_status') }}
)

select 
  name,
  type1,
  total base_status_score,
  tbd.no_damage_to+(tbd.half_damage_to/2)+(tbd.double_damage_to*2)-tbd.no_damage_from-(tbd.half_damage_from/2)-tbd.double_damage_from damage_score,
  pgr.growth_name growth_level_rate,
  pa.count_possible_moves,
  ca.count_abilities,
from pokemon_type_rank ptk
left join {{ ref('types_by_damage') }} tbd ON tbd.type = ptk.type1
left join {{ source('EL', 'pokemon_growth_rates') }} pgr on pgr.pokemon_name = ptk.name
left join (select pokemon_name, count(distinct ability_name) count_abilities from {{ source('EL', 'pokemon_abilities') }}  group by 1) ca on ca.pokemon_name = ptk.name
left join (select pokemon_name, count(distinct move_name) count_possible_moves from {{ source('EL', 'pokemon_moves') }}   group by 1) pa on pa.pokemon_name = ptk.name
where ptk.pokemon_type_rank = 1
order by damage_score DESC, total DESC