-- https://www.db-fiddle.com/f/oczEXZ43WDJCc1sYhWcLgd/0

create table customer_courier_chat_messages (
  sender_app_type varchar(32),
  customer_id bigint,
  from_id bigint,
  to_id bigint,
  chat_started_by_message boolean,
  order_id bigint,
  order_stage varchar(32),
  courier_id bigint,
  message_sent_time timestamp
);

create table orders (
  order_id bigint,
  city_code varchar(8)
);

insert into customer_courier_chat_messages values
   ('Customer iOS', 17071099, 17071099, 16293039, False, 59528555, 'PICKING_UP', 16293039, '2019-08-19
8:01:47'),
   ('Courier iOS', 17071099, 16293039, 17071099, False, 59528555, 'ARRIVING', 16293039, '2019-08-19
8:01:04'),
   ('Customer iOS', 17071099, 17071099, 16293039, False, 59528555, 'PICKING_UP', 16293039, '2019-08-19
8:00:04'),
   ('Courier Android', 12874122, 18325287, 12874122, True, 59528038, 'ADDRESS_DELIVERY', 18325287, '2019-08-19
7:59:33');

insert into orders values
  (59528555, 'NYC'),
  (59528038, 'SEA')
;

create table customer_courier_conversations as
  select * from (
    with timeline as (
    select
        order_id,
        split_part(sender_app_type, ' ', 1) as message_by,
        message_sent_time,
        order_stage,
        lead(message_sent_time) over (partition by order_id order by message_sent_time) as lead_message_sent_time,
        extract(epoch from
        lead(message_sent_time) over (
            partition by order_id order by message_sent_time
        )
        -
        message_sent_time
        ) as res_secs_elapsed,
        row_number() over (partition by order_id order by message_sent_time) as conv_message_counter,
        row_number() over (partition by order_id, split_part(sender_app_type, ' ', 1) order by message_sent_time) as by_message_counter,
        case
        when count(*) over (partition by order_id) = row_number() over (partition by order_id order by message_sent_time)
            then 1
        else 0
        end as is_last_message
    from customer_courier_chat_messages
    )
    select
      cccm.order_id,
      o.city_code,
      min(tlco.message_sent_time) as first_courier_message,
      min(tlcu.message_sent_time) as first_customer_message,
      coalesce(max(tlco.by_message_counter), 0) as num_messages_courier,
      coalesce(max(tlcu.by_message_counter), 0) as num_messages_customer,
      max(tl.message_by) as first_message_by, -- in where conv_message_counter = 1
      max(tl.message_sent_time) as conversation_started_at, -- in where conv_message_counter = 1
      max(tl.res_secs_elapsed) as first_responsetime_delay_seconds, -- in where conv_message_counter = 1
      max(tllm.message_sent_time) as last_message_time, -- in where is_last_message = 1
      max(tllm.order_stage) as last_message_order_stage -- in where is_last_message = 1
    from customer_courier_chat_messages cccm
    inner join orders o
    on cccm.order_id = o.order_id
    left join timeline tlco
    on cccm.order_id = tlco.order_id
        and tlco.message_by = 'Courier'
    left join timeline tlcu
    on cccm.order_id = tlcu.order_id
        and tlcu.message_by = 'Customer'
    left join timeline tl
    on cccm.order_id = tl.order_id
        and tl.conv_message_counter = 1
    left join timeline tllm
    on cccm.order_id = tllm.order_id
        and tllm.is_last_message = 1
    group by cccm.order_id, o.city_code
    ) q
;