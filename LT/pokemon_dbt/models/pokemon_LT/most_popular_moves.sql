select 
    pm.move_name,
    m.type,
    m.power,
    count(distinct pm.pokemon_id) count
from {{ source('EL', 'pokemon_moves') }} pm
left join {{ source('EL', 'moves') }} m on m.name = pm.move_name
group by 1,2,3
order by 4 DESC