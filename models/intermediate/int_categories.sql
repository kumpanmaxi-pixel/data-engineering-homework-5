with recursive categories as (
    select
        category_id,
        parent_category_id,
        category_name,
        category_name as root_category_name,
        1 as category_level
    from {{ ref('stg_shop__product_categories') }}
    where parent_category_id is null
    union all
    select
        c.category_id,
        c.parent_category_id,
        c.category_name,
        p.root_category_name,
        p.category_level + 1
    from {{ ref('stg_shop__product_categories') }} c
    join categories p
        on c.parent_category_id = p.category_id
)
select
    c.category_id,
    c.category_name,
    c.parent_category_id,
    p.category_name as parent_category_name,
    c.root_category_name,
    c.category_level
from categories c
left join {{ ref('stg_shop__product_categories') }} p
    on c.parent_category_id = p.category_id