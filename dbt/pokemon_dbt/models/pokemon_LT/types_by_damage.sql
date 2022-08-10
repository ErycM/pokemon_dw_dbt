
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

-- {{ config(materialized='table') }}

SELECT DISTINCT
  ty.type,
  COUNT(DISTINCT ndt.type) no_damage_to,
  COUNT(DISTINCT hdt.type) half_damage_to,
  COUNT(DISTINCT ddt.type) double_damage_to,
  COUNT(DISTINCT ndf.type) no_damage_from,
  COUNT(DISTINCT hdf.type) half_damage_from,
  COUNT(DISTINCT ddf.type) double_damage_from,
FROM {{ source('EL', 'types') }} ty
left join unnest(no_damage_to) ndt on ndt.type is not null
left join unnest(half_damage_to) hdt on hdt.type is not null
left join unnest(double_damage_to) ddt on ddt.type is not null
left join unnest(no_damage_from) ndf on ndf.type is not null
left join unnest(half_damage_from) hdf on hdf.type is not null
left join unnest(double_damage_from) ddf on ddf.type is not null
GROUP BY 1
ORDER BY double_damage_from DESC, half_damage_from DESC

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
