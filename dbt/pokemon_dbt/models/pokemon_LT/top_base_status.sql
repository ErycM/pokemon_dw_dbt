select 
  name,
  type1,
  base_experience,
  hp_base_stat,
  attack_base_stat,
  defense_base_stat,
  special_attack_base_stat,
  special_defense_base_stat,
  speed_base_stat,
  SUM(hp_base_stat)+SUM(attack_base_stat)+SUM(defense_base_stat)+SUM(special_attack_base_stat)+SUM(special_defense_base_stat)+SUM(speed_base_stat) total
from {{ source('EL', 'pokemons') }}
group by 1,2,3,4,5,6,7,8,9
order by total DESC