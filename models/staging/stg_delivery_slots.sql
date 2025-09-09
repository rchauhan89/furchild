{{ config(
  schema='SILVER',
  materialized='table',
  alias='STG_DELIVERY_SLOTS'
) }}

with raw as (
  select lower(
           trim(
             regexp_replace(
               replace(replace(cast("DELIVERY_TIME" as string),'–','-'),'—','-'),
               '\\s+',' '
             )
           )
         ) as slot_txt
  from {{ source('bronze','transactions') }}
  where "DELIVERY_TIME" is not null
    and trim(cast("DELIVERY_TIME" as string)) <> ''
),
dedup as (
  select distinct slot_txt from raw
),
parsed as (
  select
    slot_txt,

    /* flags */
    (slot_txt like '%am%') as has_am,
    (slot_txt like '%pm%') as has_pm,
    (slot_txt like '%morning%') as has_morning,
    (slot_txt like '%afternoon%') as has_afternoon,
    (slot_txt like '%evening%') as has_evening,

    /* extract 1st and 2nd numeric tokens = hours */
    regexp_substr(slot_txt, '\\d{1,2}', 1, 1) :: int as t1_hh_raw,
    regexp_substr(slot_txt, '\\d{1,2}', 1, 2) :: int as t2_hh_raw,

    /* extract minutes if present (first and second occurrences) */
    regexp_substr(slot_txt, ':(\\d{2})', 1, 1, 'e', 1) as t1_mm_raw,
    regexp_substr(slot_txt, ':(\\d{2})', 1, 2, 'e', 1) as t2_mm_raw
  from dedup
),
calc as (
  select
    slot_txt, has_am, has_pm, has_morning, has_afternoon, has_evening,
    coalesce(t1_hh_raw, 0) as t1_hh_raw,
    coalesce(t2_hh_raw, 0) as t2_hh_raw,
    coalesce(t1_mm_raw, '00') as t1_mm_raw,
    coalesce(t2_mm_raw, '00') as t2_mm_raw,

    /* pick a single hint to convert to 24h */
    iff(has_am and not has_pm, 'am',
      iff(has_pm and not has_am, 'pm',
        iff(has_morning, 'am',
          iff(has_evening or has_afternoon, 'pm', null)
        )
      )
    ) as hint
  from parsed
),
to24 as (
  select
    slot_txt, t1_mm_raw, t2_mm_raw, hint,

    -- clamp hours into [0,23] even if text has 30, 60, etc.
    lpad(
      iff(hint='am', iff(t1_hh_raw=12, 0, t1_hh_raw),
          iff(hint='pm', iff(t1_hh_raw<12, t1_hh_raw+12, t1_hh_raw),
              least(greatest(t1_hh_raw,0),23)
          )
      )::string, 2, '0'
    ) as t1_hh24,
    lpad(
      iff(hint='am', iff(t2_hh_raw=12, 0, t2_hh_raw),
          iff(hint='pm', iff(t2_hh_raw<12, t2_hh_raw+12, t2_hh_raw),
              least(greatest(t2_hh_raw,0),23)
          )
      )::string, 2, '0'
    ) as t2_hh24,

    -- helpful flags
    (slot_txt ilike '%am%' or slot_txt ilike '%pm%') as has_ampm,
    (slot_txt ilike '%min%') as has_minutes_word
  from calc
),

canon as (
  select
    slot_txt,
    -- require two numeric tokens AND either am/pm OR a colon; skip pure duration strings like "30-60 min"
    case
      when regexp_count(slot_txt, '\\d') >= 2
           and (has_ampm or slot_txt like '%:%')
        then t1_hh24||':'||lpad(t1_mm_raw,2,'0')||'-'||t2_hh24||':'||lpad(t2_mm_raw,2,'0')
      else null
    end as delivery_slot,

    try_to_time(iff(regexp_like(t1_hh24, '^(0\\d|1\\d|2[0-3]|\\d)$'),
                    t1_hh24||':'||lpad(t1_mm_raw,2,'0')||':00', null)) as start_time,

    try_to_time(iff(regexp_like(t2_hh24, '^(0\\d|1\\d|2[0-3]|\\d)$'),
                    t2_hh24||':'||lpad(t2_mm_raw,2,'0')||':00', null)) as end_time
  from to24
  -- skip cases that clearly describe a duration, not a window
  where not (has_minutes_word and not has_ampm and position(':' in slot_txt) = 0)
),
final as (
  select
    slot_txt,
    delivery_slot,
    start_time,
    end_time,
    case
      when extract(hour from start_time) between 5 and 11 then 'morning'
      when extract(hour from start_time) between 12 and 16 then 'afternoon'
      when extract(hour from start_time) between 17 and 21 then 'evening'
      else 'other'
    end as slot_period
  from canon
)

select
  {{ dbt_utils.generate_surrogate_key(['delivery_slot']) }} as delivery_slot_id,
  slot_txt,              -- keep normalized raw text for joining
  delivery_slot,         -- canonical HH:MM-HH:MM
  start_time,
  end_time,
  slot_period
from final
